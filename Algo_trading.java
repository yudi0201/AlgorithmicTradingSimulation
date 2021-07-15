import streamql.QL;
import streamql.algo.*;
import streamql.query.*;

import java.util.*;

import java.io.BufferedReader;  
import java.io.FileReader;  
import java.io.IOException; 

public class Algo_trading {
    static class Stock_data{
        private long date;
        private double open;
        private double high;
        private double low;
        private double close;
        private long volume;
        private String name;

        private Stock_data(long date, double open, double high, double low, double close, long volume, String name){
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.name = name;
        }
    }

    static class Reduced_data{
        private long date;
        private double close;
        private String name;

        private Reduced_data(long date, double close, String name){
            this.date = date;
            this.close = close;
            this.name = name;
        }
    }

    static class Moving_average{
        private String name;
        private long result_date;
        private double average;

        private Moving_average(String name, long result_date, double average){
            this.name = name;
            this.result_date = result_date;
            this.average = average;
        }
    }

    static class Final_result{
        private String name;
        private long result_date;
        private Boolean buy;

        private Final_result(String name, long result_date, Boolean buy){
            this.name = name;
            this.result_date = result_date;
            this.buy = buy;
        }


    }


    static class SigGen {
        ArrayList<Stock_data> source; 
        Iterator<Stock_data> stream; 

        public SigGen(){        
            source = new ArrayList<Stock_data>();
            String line = "";

            try   
            {  
                BufferedReader br = new BufferedReader(new FileReader("./data/100_thousand_stock_UNIX_sorted.csv")); 
                br.readLine(); 
                while ((line = br.readLine()) != null)   
                {  
                String[] row = line.split(",");    // use comma as separator  
                source.add(new Stock_data(Long.parseLong(row[0]),Double.parseDouble(row[1]),Double.parseDouble(row[2]),Double.parseDouble(row[3]),Double.parseDouble(row[4]),Long.parseLong(row[5]), row[6]));
                    } 
                br.close(); 
            }   
            catch (IOException e)   
            {  
                e.printStackTrace();  
            }
            
            stream = source.iterator();
            
        }

        public Stock_data getStockData(){
            if (this.stream.hasNext()){
                return stream.next();
            }
            else{
                return null;
            }  
        }
    }

    static Moving_average calculate_average(utils.structures.Arr<Reduced_data> x){
        int length = x.len();
        double sum = 0;

        for (int i = 0; i < length; i++){
            sum += x.get(i).close;
        }
        
        double average = sum/length;

        return new Moving_average(x.get(0).name, x.get(length-1).date, average);

    }


    public static void main(String[] args) {
        SigGen src = new SigGen();

         Sink<Final_result> sink = new Sink<Final_result> (){
            @Override
            public void next(Final_result item) {
                //Uncomment the following to see the output. Comment it to measure execution time.
                //System.out.println("Date: " + item.result_date + ", " + "Name: " + item.name + ", " + "Buy: " + item.buy);
            }
            @Override
            public void end() {
              System.out.println("Job Done");
            }
          };       

          int short_windowsize = 20;
          int long_windowsize = 50;
          Q<Stock_data, Reduced_data> f = QL.map(x->new Reduced_data(x.date, x.close, x.name));

          Q<Reduced_data, Moving_average> MAshort = QL.sWindowN(short_windowsize, x->calculate_average(x));
          Q<Reduced_data, Moving_average> MAlong = QL.sWindowN(long_windowsize, x->calculate_average(x));
          
          Q<Reduced_data, Final_result> each_company = QL.pipeline(QL.parallel(MAshort,MAlong), QL.skip(long_windowsize - short_windowsize), QL.tWindow2((x,y)->new Final_result(x.name,x.result_date,x.average>y.average)));
          Q<Reduced_data, Final_result> myres = QL.groupBy(x->x.name, each_company);
          Q<Stock_data, Final_result> res = QL.pipeline(f,myres); 

        // execution 
        Algo<Stock_data, Final_result> algo = res.eval();
        algo.connect(sink);
        algo.init();

        Long startTime = System.nanoTime();
        Stock_data curr = src.getStockData();
        while (curr != null) {
            algo.next(curr);
            curr = src.getStockData();
        };
        algo.end();

        Long endTime = System.nanoTime();
        System.out.println(endTime-startTime); 

    }
}
