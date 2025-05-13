CREATE TABLE 'dukascopy_instruments' ( 
	instrument_id VARCHAR,
	group_id VARCHAR,
	name VARCHAR,
	description VARCHAR,
	decimalFactor INT,
	startHourForTicks DATE,
	startDayForMinuteCandles DATE,
	startMonthForHourlyCandles DATE,
	startYearForDailyCandles DATE
)
WITH maxUncommittedRows=500000, o3MaxLag=600000000us;


CREATE TABLE 'tv_symbols_us' ( 
	namespace VARCHAR,
	ticker VARCHAR,
	full VARCHAR
)
WITH maxUncommittedRows=500000, o3MaxLag=600000000us;
