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
				.Size(25)
				.Query(q => q
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
					)
				)
			);

			var model = new SearchViewModel
			{
				Hits = result.Hits,
				Total = result.Total,
				Form = form
			};

	        return View(model);
		}
    }
}
