using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nest;
using NuSearch.Domain;
using NuSearch.Domain.Data;
using NuSearch.Domain.Model;

namespace NuSearch.Indexer
{
	class Program
	{
		private static ElasticClient Client { get; set; }
		private static NugetDumpReader DumpReader { get; set; }
		private static string CurrentIndexName { get; set; }

		static void Main(string[] args)
		{
			Client = NuSearchConfiguration.GetClient();
			var directory = args.Length > 0 && !string.IsNullOrEmpty(args[0]) 
				? args[0] 
				: NuSearchConfiguration.PackagePath;
			DumpReader = new NugetDumpReader(directory);
			CurrentIndexName = NuSearchConfiguration.CreateIndexName();
			
			CreateIndex();
			IndexDumps();
			SwapAlias();

			Console.WriteLine("Press any key to exit.");
			Console.ReadKey();
		}

		static void IndexDumps()
		{
			Console.WriteLine("Setting up a lazy xml files reader that yields packages...");
			var packages = DumpReader.GetPackages();//.Take(1000);

			Console.Write("Indexing documents into Elasticsearch...");
			var waitHandle = new CountdownEvent(1);

			var bulkAll = Client.BulkAll(packages, b => b
				.Index(CurrentIndexName)
				.BackOffRetries(2)
				.BackOffTime("30s")
				.RefreshOnCompleted(refresh: true)
				.MaxDegreeOfParallelism(parallism: 4)
				.Size(size: 1000)
			);

			bulkAll.Subscribe(new BulkAllObserver(
				onNext: b => Console.Write("."),
				onError: e => throw e,
				onCompleted: () => waitHandle.Signal()
				));

			waitHandle.Wait();

			Console.WriteLine("Done.");
		}

		private static void CreateIndex()
		{
			const string NUGET_ID_TOKENIZER = "nuget-id-tokenizer";
			const string NUGET_ID_WORDS = "nuget-id-words";
			const string NUGET_ID_ANALYZER = "nuget-id-analyzer";
			const string NUGET_ID_KEYWORD = "nuget-id-keyword";

			Client.CreateIndex(CurrentIndexName, i => i
				.Settings(s => s
					.NumberOfShards(2)
					.NumberOfReplicas(0)
					.Analysis(analysis => analysis
						.Tokenizers(tokenizers => tokenizers
							// Standard tokenizer doesn't split on ".", so we'll use a custom one that splits on any 
							//   non-word character.
							.Pattern(NUGET_ID_TOKENIZER, p => p.Pattern(@"\W+"))
						)
						.TokenFilters(tokenFilters => tokenFilters
							// Customize the WordDelimiterTokenFilter to split on case change. e.g., ShebangModule will 
							//   be cut up into Shebang and Module and because we tell it to preserve the original 
							//   token, ShebangModule is kept as well.
							.WordDelimiter(NUGET_ID_WORDS, w => w
								.SplitOnCaseChange()
								.PreserveOriginal()
								.SplitOnNumerics()
								.GenerateNumberParts(false)
								.GenerateWordParts()
							)
						)
						.Analyzers(analyzers => analyzers
							// Here, we create an analyzer named nuget-id-analyzer which will split the input using our 
							//   nuget-id-tokenizer and then filter the resulting terms using our nuget-id-words filter, 
							//   finally we use the in-built lowercase token filter to replace all the terms with their 
							//   lowercase counterparts.
							.Custom(NUGET_ID_ANALYZER, c => c
								.Tokenizer(NUGET_ID_TOKENIZER)
								.Filters(NUGET_ID_WORDS, "lowercase")
							)
							// Here we create a special nuget-id-keyword analyzer. The built in Keyword Tokenizer emits 
							//   the provided text as a single term, unchanged (IOW, it's a no-op). We then use the 
							//   lowercase filter to lowercase the id as a whole. This will allow us to boost exact 
							//   matches on id higher, without the user having to know the correct casing.
							.Custom(NUGET_ID_KEYWORD, c => c
								.Tokenizer("keyword")
								.Filters("lowercase")
							)
						)
					)
				)
				.Mappings(m => m
					.Map<Package>(map => map
						.AutoMap()
						.Properties(ps => ps
							.Text(s => s
								// Here we setup our Id property as a multi field mapping. This allows us to analyze the 
								//   Id property in a number of different ways. It will index the terms from the analysis 
								//   chain into the inverted index, and allow us to target the different methods in which 
								//   Id property has been analyzed at search time.
								// At index time, id will use our nuget-id-analyzer to split the NuGet package Id into the 
								//   proper terms. When we do a match query on the id field, Elasticsearch will perform 
								//   analysis on the query input using our nuget-id-analyzer to generate the correct terms 
								//   from the provided input.
								// When we query id.keyword using the match query, Elasticsearch will simply use the whole 
								//   query, lowercase it, and use that as the search term in the id.keyword field within 
								//   the inverted index.
								// Elasticsearch also creates a field in the inverted index called id.raw. This is the raw 
								//   value which has not been analyzed. This field is well suited for sorting and 
								//   aggregations (some locales sort with case sensitivity).
								.Name(p => p.Id)
								.Analyzer(NUGET_ID_ANALYZER)
								.Fields(f => f
									.Text(p => p.Name("keyword").Analyzer(NUGET_ID_KEYWORD))
									.Keyword(p => p.Name("raw"))
								)
							)
							.Nested<PackageVersion>(n => n
								.Name(p => p.Versions.First())
								.AutoMap()
								.Properties(pps => pps
									.Nested<PackageDependency>(nn => nn
										.Name(pv => pv.Dependencies.First())
										.AutoMap()
									)
								)
							)
							.Nested<PackageAuthor>(n => n
								.Name(p => p.Authors.First())
								.AutoMap()
								.Properties(props => props
									.Text(t => t
										.Name(a => a.Name)
										.Fielddata()
										.Fields(fs => fs
											.Keyword(ss => ss.Name("raw")
											)
										)
									)
								)
							)
						)
					)
				)
			);
		}

		private static void SwapAlias()
		{
			//TODO: check the return type of this instead of just accepting the boolean.
			var indexExists = Client.IndexExists(NuSearchConfiguration.LiveIndexAlias).Exists;

			// We have to use the actual index name, not the live index alias, when adding the "old" alias
			//   to the existing index.
			var indicesPointingToAlias = new List<string>();
			if (indexExists)
			{
				indicesPointingToAlias.AddRange(Client.GetIndicesPointingToAlias(NuSearchConfiguration.LiveIndexAlias));
			}

			var response = Client.Alias(aliases =>
			{
				if (indicesPointingToAlias.Count > 0)
				{
					// There should only be one, but loop through just in case we somehow ended up with more.
					foreach (var index in indicesPointingToAlias)
					{
						var tempIndex = index;
						aliases.Add(a => a.Alias(NuSearchConfiguration.OldIndexAlias).Index(tempIndex));
					}
				}

				return aliases
					.Remove(a => a.Alias(NuSearchConfiguration.LiveIndexAlias).Index("*"))
					.Add(a => a.Alias(NuSearchConfiguration.LiveIndexAlias).Index(CurrentIndexName));
			});

			// Break it apart for debugging purposes.
			var oldIndices = Client.GetIndicesPointingToAlias(NuSearchConfiguration.OldIndexAlias)
				.OrderByDescending(name => name)
				.ToList();

			oldIndices = oldIndices
				.Skip(2)
				.ToList();

			foreach (var oldIndex in oldIndices)
			{
				Client.DeleteIndex(oldIndex);
			}
		}
	}
}
