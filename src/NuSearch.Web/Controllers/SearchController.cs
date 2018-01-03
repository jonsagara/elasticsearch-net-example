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
									// For searching on "id.keywrod", which we defined in the index (i.e., id has multi-field mapping).
									.Field(p => p.Id.Suffix("keyword"), 1.5)
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
