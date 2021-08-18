using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
using System.Xml.Xsl;
using Microsoft.StreamProcessing;

namespace AlgoTradingTrill
{
    class Program
    {
        class StockPriceDaily
        {
            public long Date;
            public double Open;
            public double High;
            public double Low;
            public double Close;
            public long Volume;
            public string Name;

            public StockPriceDaily(long date, double open, double high, double low, double close, long volume, string name)
            {
                this.Date = date;
                this.Open = open;
                this.High = high;
                this.Low = low;
                this.Close = close;
                this.Volume = volume;
                this.Name = name;
            }
        }
        
        class MyObservable : IObservable<StockPriceDaily>
        {
            public IDisposable Subscribe(IObserver<StockPriceDaily> observer)
            {
                //using (var reader = new StreamReader(@"C:\Users\yudis\Documents\university\Summer2021\Code\AlgorithmicTradingSimulation\data\10_million_stock_UNIX_sorted.csv"))
                using (var reader = new StreamReader(@"AlgorithmicTradingSimulation/data/100_thousand_stock_UNIX_single_firm.csv"))
                {
                    reader.ReadLine();
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(',');
                        //Console.WriteLine(line);
                        var data = new StockPriceDaily(long.Parse(values[0]),Convert.ToDouble(values[1]),Convert.ToDouble(values[2]),
                            Convert.ToDouble(values[3]), Convert.ToDouble(values[4]),long.Parse(values[5]), values[6]);
                        observer.OnNext(data);

                    }
                }
                observer.OnCompleted();
                return Disposable.Empty;
            }
        }
        
        static void Main(string[] args)
        {
            //var stockRecordObservable = new[]
            //{
            //    //This should be done in a separate file, but I still need to learn how to read files in C#.
            //    new StockPriceDaily(1483401600, 62.79, 62.84, 62.12, 62.58, 20694101, "MSFT"),
            //    new StockPriceDaily(1483488000, 62.48,62.75,62.12,62.3,21339969,"MSFT"),
            //    new StockPriceDaily(1483574400, 62.19,62.66,62.03,62.3,21339969,"MSFT"),
            //    new StockPriceDaily(1483660800, 62.3,63.15,62.04,62.84,21339969,"MSFT"),
            //    new StockPriceDaily(1483920000, 62.76,63.08,62.54,62.64,21339969,"MSFT"),
            //    new StockPriceDaily(1484006400, 62.73,63.07,62.28,62.62,21339969,"MSFT"),
            //    new StockPriceDaily(1484092800, 62.61,63.23,62.43,63.19,21339969,"MSFT"),
            //    new StockPriceDaily(1484179200, 63.06,63.4,61.95,62.61,21339969,"MSFT"),
            //    new StockPriceDaily(1484265600, 62.62,62.86,62.35,62.7,21339969,"MSFT"),
            //    new StockPriceDaily(1484611200, 62.68,62.7,62.03,62.53,21339969,"MSFT"),
            //    new StockPriceDaily(1484697600, 62.67,62.7,62.12,62.5,21339969,"MSFT"),
            //    new StockPriceDaily(1484784000, 62.24,62.98,62.2,62.3,21339969,"MSFT"),
            //    new StockPriceDaily(1484870400, 62.67,62.82,62.37,62.74,21339969,"MSFT"),
            //    new StockPriceDaily(1485129600, 62.7,63.12,62.57,62.96,21339969,"MSFT"),
            //    new StockPriceDaily(1485216000, 63.2,63.74,62.94,63.52,21339969,"MSFT"),
            //    new StockPriceDaily(1485302400, 63.95,64.1,63.45,63.68,21339969,"MSFT"),
            //    new StockPriceDaily(1485388800, 64.12,64.54,63.55,64.27,21339969,"MSFT"),
            //    new StockPriceDaily(1485475200, 65.39,65.91,64.89,65.78,21339969,"MSFT"),
            //    new StockPriceDaily(1485734400, 65.69,65.79,64.8,65.13,21339969,"MSFT"),
            //    new StockPriceDaily(1485820800, 64.86,65.15,64.26,64.65,21339969,"MSFT")
            //}.ToObservable();
            
            //var stockRecordStreamable =
            //    stockRecordObservable.Select(e => StreamEvent.CreateInterval(e.Date, e.Date + 86400, e))
            //        .ToStreamable(DisorderPolicy.Drop());
            
            var sw = new Stopwatch();
            
            var stockRecordObservable = new MyObservable();
            var stockRecordStreamable =
                stockRecordObservable.ToTemporalStreamable(e => e.Date, e => e.Date + 1).Cache();
            
            //var allPrices =
            //    stockRecordStreamable.Select(e => new{e.Open, e.High, e.Low, e.Close, e.Name});

            var movingAverages = stockRecordStreamable.Multicast(
                    t => t.HoppingWindowLifetime(50, 1)
                        .Average(e => e.Close).Join(t
                                .HoppingWindowLifetime(20, 1).Average(e => e.Close),
                            (ma50, ma20) => new {ma50, ma20}));

            var result = movingAverages.Multicast(s => s.Join(s.Select(e => e.ma20 > e.ma50),
                (left, buy) => new {left, buy}));
               
            
            //result
            //    .ToStreamEventObservable() 
            //    .Where(e => e.IsData) 
            //    .ForEachAsync(e => Console.WriteLine(e.ToString()));
            
            sw.Start();
            result
                .ToStreamEventObservable()
                .Wait();
            
            sw.Stop();
            Console.WriteLine(sw.Elapsed.TotalSeconds);
        }
    }
    

}