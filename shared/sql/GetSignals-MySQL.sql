CREATE DEFINER=`root`@`localhost` PROCEDURE `GetActiveTradingSignals`()
BEGIN
    -- 1. Identify the 'Current' and 'Previous' time markers based on data
    SET @current_tick = (SELECT MAX(PriceTime) FROM fourhour);
    #set @current_tick = '2026-02-06 08:00:00';
    #SET @prev_tick = (SELECT MAX(PriceTime) FROM fourhour WHERE PriceTime < @current_tick);
    SET @prev_tick = DATE_ADD(@current_tick, INTERVAL -4 HOUR);
	#select @current_tick , @prev_tick ;
     
    
    -- 2. Select signals where current metrics cross the 'Natural Flow' thresholds
    SELECT 
    curr.CoinSymbol,
    curr.PriceTime AS LastPriceTime,
    prev.PriceTime AS PreviousPriceTime,
    curr.ZScore AS LastZScore,
    prev.ZScore AS PreviousZScore,
    curr.RSI AS LastRSI,
    prev.RSI AS PreviousRSI,
    curr.ADX,
    'FourHour' as TimeInterval,
    CASE 
        -- CORRECTED LONG: Cross back above -2 from below
        WHEN prev.ZScore <= -2.0 AND curr.ZScore > -2.0   AND curr.RSI > prev.RSI  and ABS(curr.RSI - prev.RSI) > 1 THEN 'LONG'
        -- CORRECTED SHORT: Cross back below +2 from above
        WHEN prev.ZScore >= 2.0 AND curr.ZScore < 2.0  AND curr.RSI < prev.RSI  and ABS(curr.RSI - prev.RSI) > 1 THEN 'SHORT'
    END AS TradeOrderPriceDirection,
    CASE 
            WHEN (prev.ZScore < -2.0) THEN ROUND(curr.ClosePrice * 1.05, 8) -- LONG TP
            ELSE ROUND(curr.ClosePrice * 0.95, 8) -- SHORT TP
        END AS TargetPrice,
        CASE 
            WHEN (prev.ZScore < -2.0) THEN ROUND(curr.ClosePrice * 0.94, 8) -- LONG SL
            ELSE ROUND(curr.ClosePrice * 1.06, 8) -- SHORT SL
        END AS StopLossPrice,
        curr.ClosePrice AS CurrentPrice,
        DATE_ADD(curr.PriceTime, INTERVAL 12 HOUR) AS TimeExit
    FROM fourhour curr
	JOIN fourhour prev ON curr.CoinSymbol = prev.CoinSymbol
	WHERE curr.PriceTime = @current_tick
	  AND prev.PriceTime = @prev_tick
	  AND curr.ADX < 21  -- Mandatory Range Filter
	  AND curr.ADX > 9
	  and ABS(curr.RSI - prev.RSI) > 1 
	  and prev.ADX < curr.ADX
	  AND (
		  (prev.ZScore <= -1.95 AND curr.ZScore > -2.05 AND curr.RSI > prev.RSI ) -- Long Check
		  OR 
		  (prev.ZScore >= 1.95 AND curr.ZScore < 2.05 AND curr.RSI < prev.RSI )   -- Short Check
	  ) order by curr.ADX, (curr.RSI - prev.RSI) desc, abs(curr.ZScore - prev.ZScore) desc;
 

END