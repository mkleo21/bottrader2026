CREATE OR ALTER PROCEDURE [dbo].[GetActiveTradingSignals]
AS
BEGIN
    SET NOCOUNT ON;

    -- 0. Ensure SignalsTable exists
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SignalsTable')
    BEGIN
        CREATE TABLE SignalsTable (
            SignalID INT IDENTITY(1,1) PRIMARY KEY,
            CoinSymbol VARCHAR(20),
            PriceDateTime DATETIME,
            Zscore FLOAT,
            RSI FLOAT,
            ADX FLOAT,
            Direction VARCHAR(10),
            CurrentPrice FLOAT,
            TargetPrice FLOAT,
            StopLossPrice FLOAT,
            CreatedDateTime DATETIME DEFAULT GETDATE(),
            CONSTRAINT UC_Signal UNIQUE (CoinSymbol, PriceDateTime)
        );
    END

    -- 1. Identify the 'Current' and 'Previous' time markers based on data
    DECLARE @current_tick DATETIME;
    SELECT @current_tick = MAX(PriceDateTime) FROM FourHour;
    
    DECLARE @prev_tick DATETIME;
    SET @prev_tick = DATEADD(HOUR, -4, @current_tick);

    -- 2. Use a CTE to identify valid signals
    WITH RawSignals AS (
        SELECT 
            curr.CoinSymbol,
            curr.PriceDateTime,
            curr.Zscore,
            curr.RSI,
            curr.ADX,
            CASE 
                WHEN prev.Zscore <= -2.0 AND curr.Zscore > -2.0 AND curr.RSI > prev.RSI AND ABS(curr.RSI - prev.RSI) > 1 THEN 'LONG'
                WHEN prev.Zscore >= 2.0 AND curr.Zscore < 2.0 AND curr.RSI < prev.RSI AND ABS(curr.RSI - prev.RSI) > 1 THEN 'SHORT'
            END AS Direction,
            curr.ClosePrice AS CurrentPrice,
            CASE 
                WHEN (prev.Zscore < -2.0) THEN ROUND(curr.ClosePrice * 1.05, 8) -- LONG TP
                ELSE ROUND(curr.ClosePrice * 0.95, 8) -- SHORT TP
            END AS TargetPrice,
            CASE 
                WHEN (prev.Zscore < -2.0) THEN ROUND(curr.ClosePrice * 0.94, 8) -- LONG SL
                ELSE ROUND(curr.ClosePrice * 1.06, 8) -- SHORT SL
            END AS StopLossPrice,
            prev.ADX as PrevADX,
            prev.RSI as PrevRSI,
            prev.Zscore as PrevZscore
        FROM FourHour curr
        JOIN FourHour prev ON curr.CoinSymbol = prev.CoinSymbol
        WHERE curr.PriceDateTime = @current_tick
          AND prev.PriceDateTime = @prev_tick
    )
    , FilteredSignals AS (
        SELECT *
        FROM RawSignals
        WHERE Direction IS NOT NULL
          AND ADX < 21 
          AND ADX > 9
          AND ABS(RSI - PrevRSI) > 1 
          AND PrevADX < ADX
          AND (
              (PrevZscore <= -1.95 AND Zscore > -2.05 AND RSI > PrevRSI) -- Long Check
              OR 
              (PrevZscore >= 1.95 AND Zscore < 2.05 AND RSI < PrevRSI)   -- Short Check
          )
    )
    -- 3. Log signals into SignalsTable (ignore duplicates via WHERE NOT EXISTS)
    INSERT INTO SignalsTable (CoinSymbol, PriceDateTime, Zscore, RSI, ADX, Direction, CurrentPrice, TargetPrice, StopLossPrice)
    SELECT CoinSymbol, PriceDateTime, Zscore, RSI, ADX, Direction, CurrentPrice, TargetPrice, StopLossPrice
    FROM FilteredSignals fs
    WHERE NOT EXISTS (
        SELECT 1 FROM SignalsTable st 
        WHERE st.CoinSymbol = fs.CoinSymbol 
          AND st.PriceDateTime = fs.PriceDateTime
    );

    -- 4. Return results for the caller
    SELECT 
        CoinSymbol,
        PriceDateTime AS LastPriceTime,
        DATEADD(HOUR, -4, PriceDateTime) AS PreviousPriceTime,
        Zscore AS LastZscore,
        -- We don't have the prev metrics in the final select easily without joining again 
        -- but the caller mainly needs the current ones for setup.
        -- Keeping output consistent with previous version
        RSI AS LastRSI,
        ADX,
        'FourHour' AS TimeInterval,
        Direction AS TradeOrderPriceDirection,
        TargetPrice,
        StopLossPrice,
        CurrentPrice,
        DATEADD(HOUR, 12, PriceDateTime) AS TimeExit
    FROM FilteredSignals
    ORDER BY ADX, (RSI - PrevRSI) DESC, ABS(Zscore - PrevZscore) DESC;
END
GO
