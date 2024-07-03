// See https://aka.ms/new-console-template for more information

using Couchbase;
using Couchbase.Transactions.Config;
using Couchbase.Transactions;
using Couchbase.Transactions.Error;
using Couchbase.KeyValue;
using System.Diagnostics;

Console.WriteLine("Hello, World!");

await new StartUsing().Main(args[0], args[1], args[2], args[3], args[4]);
//await new StartUsing().BulkInsertInTxn();
class StartUsing
{

    private static Random random = new Random();

    public static string RandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }
   // public async Task Main(string host, string username, string password, string totalS, string chunkSizeS)
   public async Task Main(string host, string username, string password, string totalS, string sizeS)
    {
        var total = Int32.Parse(totalS);

        var options = new ClusterOptions().WithCredentials(username, password);
        var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test").ConfigureAwait(false);
        var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);
        var _collection = await scope.CollectionAsync("test").ConfigureAwait(false);

        // Create the single Transactions object
        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .DurabilityLevel(DurabilityLevel.None)
        .ExpirationTime(TimeSpan.FromHours(1))
        .Build());

        var documento = new
        {
            _id = "667bfaddb0463c180d804cc9",
            index = 0,
            guid = "5225fdb9-b9c4-4887-8220-7a5eca255531",
            isActive = true,
            balance = "$1,022.47",
            picture = "http=//placehold.it/32x32",
            age = 39,
            body = RandomString(Int32.Parse(sizeS)),
            eyeColor = "brown",
            name = "Sherri Burke",
            gender = "female",
            company = "ZILLANET",
            email = "sherriburke@zillanet.com"
        };


        try
        {

            
                 var watch = Stopwatch.StartNew();


                 var result = await _transactions.RunAsync(async (ctx) => 
                {

                        await Parallel.ForEachAsync(Enumerable.Range(0, total), async (index, token) =>
                    {
                        var opt = await ctx.GetOptionalAsync(_collection, index.ToString()).ConfigureAwait(false);
                        if (opt == null)
                            await ctx.InsertAsync(_collection, index.ToString(), documento).ConfigureAwait(false);
                        else
                            await ctx.ReplaceAsync(opt, documento).ConfigureAwait(false);

                        if (index % 100 == 0)
                        {
                                         Console.Clear();
                                         Console.Write($"Staged {index} documents");
                        }
                    }).ConfigureAwait(false);

                    await ctx.CommitAsync().ConfigureAwait(false);
                }).ConfigureAwait(false);
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                Console.Clear();
                Console.WriteLine(elapsedMs / 1000 + "s");
            }
            catch (TransactionCommitAmbiguousException e)
            {
                Console.WriteLine("Transaction possibly committed");
                Console.WriteLine(e);
            }
            catch (TransactionFailedException e)
            {
                Console.WriteLine("Transaction did not reach commit point");
                Console.WriteLine(e);
            }

            

        
    }

    // sequential version

    //var stopwatch = Stopwatch.StartNew();
    //try
    //{
    //    var result = await _transactions.RunAsync(async (ctx) =>
    //    {
    //        for (int i = 0; i < 10_000; i++)
    //        {
    //            await ctx.InsertAsync(_collection, $"testDocument2{i}", documento).ConfigureAwait(false);
    //            Console.Clear();
    //            Console.Write($"Staged {i} documents. Time elapsed: {stopwatch.Elapsed.TotalSeconds}s");
    //        }
    //        Console.WriteLine($"Staging documents:{stopwatch.Elapsed.TotalSeconds}s, or {stopwatch.Elapsed.TotalMinutes}min");
    //        stopwatch.Restart();
    //        await ctx.CommitAsync().ConfigureAwait(false);
    //    }).ConfigureAwait(false);
    //}
    //catch (Exception e)
    //{
    //    Console.WriteLine(e);
    //}
    //stopwatch.Stop();
    //Console.WriteLine($"Committing documents:{stopwatch.Elapsed.TotalSeconds}s, or {stopwatch.Elapsed.TotalMinutes}min");

}