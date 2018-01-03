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
			var packages = DumpReader.GetPackages().Take(1000);

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
			Client.CreateIndex(CurrentIndexName, i => i
				.Settings(s => s
					.NumberOfShards(2)
					.NumberOfReplicas(0)
				)
				.Mappings(m => m
					.Map<Package>(map => map
						.AutoMap()
						.Properties(ps => ps
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
