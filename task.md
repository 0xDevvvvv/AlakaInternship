***************************************************************************************************************************************** 

* For a single file (strike-price), consider the data only for 10th Jan 2024 (single file has data for multiple days; drop rest of the days) and calculate the following and store it another folder named 5min_candles as CSV. 
    * 5 minute candles for the dataset for 10th Jan 2024 
        * Candle attributes are as follows: 
            Open: First value within the time window. 
            High: Maximum value within the time window. 
            Low: Minimum value within the time window. 
            Close: Last value within the time window. 

* Repeat the exercise for rest of the files in the folder and store the 5-minute candles in a folder next to the data files named 5min_candles as CSV. 

BONUS FEATURE 

* Fibonacci Pivot points for the dataset for 10th Jan 2024. Fibonacci Pivot points uses the same logic for candles as above but the time window will be for the entire day. 
    Post calculation the values of Pivot, R1, R2, R3, S1, S2, S3 for the dataset must be displayed 
    Formula 
        Pivot Point Formula: 
            Pivot Point (P)=(High+Low+Close)/3 
        Resistance and Support Levels: 
        Using the pivot point (P), calculate the following levels: 

        R1 (Resistance 1): P+0.382×(High−Low) 
        R2 (Resistance 2): P+0.618×(High−Low) 
        R3 (Resistance 3): P+(High−Low) 

        S1 (Support 1): P−0.382×(High−Low) 
        S2 (Support 2): P−0.618×(High−Low) 
        S3 (Support 3): P−(High−Low) 

***************************************************************************************************************************************** 