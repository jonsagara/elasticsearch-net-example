using System;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using Nest;
using NuSearch.Domain.Model;
using NuSearch.Web.Models;

namespace NuSearch.Web.Controllers
{
	public class SearchController : Controller
    {
		private readonly IElasticClient _client;

		public SearchController(IElasticClient client) => _client = client;

	    [HttpGet]
        public IActionResult Index(SearchForm form)
        {
			var result = _client.Search<Package>(s => s
				.Query(q => (q
					// Here we use the logical || operator to create a bool query to match either on id.keyword and if it 
					//   does, give it a large boost, or otherwise match with our function_score query.
					.Match(m => m
						.Field(p => p.Id.Suffix("keyword"))
						.Boost(1000)
						.Query(form.Query)
					) || q
					.FunctionScore(fs => fs
						// Use download count to boost a document's score, but cap this boost at 50 so that packages with
						//   a download count over 500k don't differentiate on downloadCount.
						.MaxBoost(50)
						.Functions(ff => ff
							.FieldValueFactor(fvf => fvf
								.Field(p => p.DownloadCount)
								.Factor(0.0001)
							)
						)
						.Query(query => query
							.MultiMatch(m => m
								.Fields(f => f
									.Field(p => p.Id, 1.5)
									.Field(p => p.Summary, 0.8)
								)
								.Operator(Operator.And)
								.Query(form.Query)
							)
						)
					))
					// The + before q.Nested automatically wraps the query in a bool query filter clause, so as not to 
					//   calculate a score for the query
					&& +q.Nested(n => n
						.Path(p => p.Authors)
						.Query(nq => +nq
							.Term(p => p.Authors.First().Name.Suffix("raw"), form.Author)
						)
					)
				)
				.From((form.Page - 1) * form.PageSize)
				.Size(form.PageSize)
				.Sort(sort =>
				{
					// If the sort order is downloads, we do a descending sort on our downloadcount field.
					if (form.Sort == SearchSort.Downloads)
					{
						return sort.Descending(p => p.DownloadCount);
					}

					// If the sort order is most recently updated we need to do a descending nested sort on p.Versions.First().LastUpdated 
					//   because we mapped Versions as a nested object array in the previous module.
					if (form.Sort == SearchSort.Recent)
					{
						return sort.Field(sortField => sortField
							.NestedPath(p => p.Versions)
							.Field(p => p.Versions.First().LastUpdated)
							.Descending()
						);
					}

					// Otherwise we sort descending by "_score", which is the default behaviour. Returning null here is also an option.
					return sort.Descending(SortSpecialField.Score);
				})
				.Aggregations(a => a
					.Nested("authors", n => n
						.Path(p => p.Authors)
						.Aggregations(aa => aa
							.Terms("author-names", ts => ts
								.Field(p => p.Authors.First().Name.Suffix("raw"))
							)
						)
					)
				)
			);

			var authors = result.Aggs
				.Nested("authors")
				.Terms("author-names")
				.Buckets
				.ToDictionary(k => k.Key, v => v.DocCount);

			var model = new SearchViewModel
			{
				Hits = result.Hits,
				Total = result.Total,
				Form = form,
				TotalPages = (int)Math.Ceiling(result.Total / (double)form.PageSize),
				Authors = authors
			};

	        return View(model);
		}
    }
}
