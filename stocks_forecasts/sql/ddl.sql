DROP TABLE IF EXISTS stocks;

CREATE TABLE IF NOT EXISTS stocks(
   ticker VARCHAR(20) NOT NULL PRIMARY KEY,
   figi VARCHAR(20) NULL,   
   stock_name varchar(100) NOT NULL,
   country_id varchar(3) NOT NULL,   
   currency varchar(3) NOT NULL,   
   lot int  NOT NULL,      
   class_code varchar(20) NOT NULL,
   sector varchar(50) NOT NULL,
   created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
   updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp   
);

comment on table stocks is 'Компании';
comment on column stocks.ticker is 'Тикер';
comment on column stocks.figi is 'Код бумаги в Т-Инвестиции';
comment on column stocks.stock_name is 'Название компании';
comment on column stocks.country_id is 'Страна';
comment on column stocks.currency is 'Валюта';
comment on column stocks.lot is 'Количество акций в лоте';
comment on column stocks.created_at is 'Дата создания записи';
comment on column stocks.updated_at is 'Дата обновления записи';

DROP TABLE IF EXISTS indicators;

CREATE TABLE IF NOT EXISTS indicators(
   id SERIAL PRIMARY KEY,
   indicator_name varchar(60) NOT NULL,
   success_proc int NULL,
   created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
   updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp   
);

CREATE UNIQUE INDEX indicator_name ON indicators (indicator_name);


comment on table indicators is 'Индикаторы';
comment on column indicators.indicator_name is 'Название';
comment on column indicators.success_proc  is 'Процент успешных предсказаний';
comment on column indicators.created_at is 'Дата создания записи';
comment on column indicators.updated_at is 'Дата обновления записи';



DROP TABLE IF EXISTS stocks_forecasts;

CREATE TABLE IF NOT EXISTS stocks_forecasts(
   id SERIAL PRIMARY KEY,
   analyst_company_id int NOT NULL,
   ticker VARCHAR(20) NOT NULL,
   start_date date NOT NULL,
   end_date date NOT NULL,
   forecast_direction int not null,   
   price_start numeric(16, 6) NULL,
   price_forecast numeric(16, 6) NULL,
   price_delta_forecast numeric(7, 3) NOT NULL,
   price_end numeric(16, 10) NULL,
   price_max_real numeric(16, 6) NULL,
   price_max_date timestamp NULL,
   price_min_real numeric(16, 6) NULL,
   price_min_date  timestamp NULL,
   end_real_price_delta numeric(7, 3) NULL,
   price_max_delta numeric(7, 3) NULL,
   price_min_delta numeric(7, 3) NULL,
   forecast_result smallint DEFAULT 0,
   external_id varchar(40) NULL,
   indicator_id int null,      
   created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
   updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp   
);



CREATE UNIQUE INDEX analyst_company_id_ticker_date ON stocks_forecasts (analyst_company_id, ticker, start_date);
CREATE INDEX end_date_forecast_result ON stocks_forecasts (end_date, forecast_result);

comment on table stocks_forecasts is 'Прогнозы';
comment on column stocks_forecasts.analyst_company_id is 'ID аналитической компании';
comment on column stocks_forecasts.start_date is 'Дата начала прогноза';
comment on column stocks_forecasts.end_date is 'Дата конца прогноза';
comment on column stocks_forecasts.ticker is 'Тикер';
comment on column stocks_forecasts.forecast_direction is 'Направление прогноза - рост (1) или падение(2)';
comment on column stocks_forecasts.price_start is 'Цена на начало прогноза';
comment on column stocks_forecasts.price_forecast is 'Прогнозная цена';
comment on column stocks_forecasts.price_delta_forecast is 'Насколько цена должна вырасти/упасть';
comment on column stocks_forecasts.price_end is 'Реальная цена на конец прогноза';
comment on column stocks_forecasts.price_max_real is 'Максимальная реальная цена на прогнозном интервале';
comment on column stocks_forecasts.price_max_date is 'Дата максимальной цены на прогнозном интервале';
comment on column stocks_forecasts.price_min_real is 'Минимальная цена на прогнозном интервале';
comment on column stocks_forecasts.price_min_date is 'Дата минимальной цены на прогнозном интервале';
comment on column stocks_forecasts.end_real_price_delta is 'Дельта реальной цены от прогнозной на конец интервала';
comment on column stocks_forecasts.price_max_delta is 'Дельта максимальной цены от прогнозной на интервале';
comment on column stocks_forecasts.price_min_delta is 'Дельта минимальной цены от прогнозной на интервале';
comment on column stocks_forecasts.forecast_result is '0 - прогноз не рассчитан, 1 - прогноз сбылся, 2 - прогноз не сбылся, 3 - цена осталась той же самой';
comment on column stocks_forecasts.indicator_id is 'FK: indicators.id';
comment on column stocks_forecasts.created_at is 'Дата создания записи';
comment on column stocks_forecasts.updated_at is 'Дата обновления записи';

DROP TABLE IF EXISTS stocks_quotes;

CREATE TABLE IF NOT EXISTS stocks_quotes(
   id SERIAL PRIMARY KEY,
   ticker VARCHAR(20) NOT NULL,
   datetime_start timestamp  not null,
   datetime_end timestamp  not null,
   open_price numeric(16, 6) NULL,
   close_price numeric(16, 6) NULL,   
   high_price numeric(16, 6) NULL,   
   low_price numeric(16, 6) NULL,   
   volume int NULL,
   created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
   updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp   
);

CREATE UNIQUE INDEX ticker_quote_date ON stocks_quotes (ticker, datetime_start);

comment on table stocks_quotes is 'Котировки';
comment on column stocks_quotes.ticker is 'Тикер';
comment on column stocks_quotes.datetime_start is 'Дата и время начала периода котировки';
comment on column stocks_quotes.datetime_end is 'Дата и время конца периода котировки';
comment on column stocks_quotes.open_price is 'Цена открытия';
comment on column stocks_quotes.close_price is 'Цена закрытия';
comment on column stocks_quotes.high_price is 'Максимальная цена';
comment on column stocks_quotes.low_price is 'Минимальная цена';
comment on column stocks_quotes.volume is 'Объеём торгов, в лотах';
comment on column stocks_quotes.created_at is 'Дата создания записи';
comment on column stocks_quotes.updated_at is 'Дата обновления записи';

DROP TABLE IF EXISTS parser_log;

CREATE TABLE IF NOT EXISTS parser_log(
   id SERIAL PRIMARY KEY,
   analyst_company_id int not null,
   start_time timestamp  NOT NULL,
   end_time timestamp  NULL,
   forecast_datetime timestamp NULL,   
   external_id varchar(40) NULL,
   created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
   updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp   
);

CREATE UNIQUE INDEX parser_log_forecast_datetime ON parser_log (forecast_datetime);

comment on table parser_log is 'Лог парсера';
comment on column parser_log.analyst_company_id is 'Id аналитической компании';
comment on column parser_log.start_time is 'Дата и время начала работы парсера';
comment on column parser_log.end_time is 'Дата и время конца работы парсера';
comment on column parser_log.forecast_datetime is 'Дата и время последнего добавленного прогноза';
comment on column parser_log.external_id is 'Дата и время последнего id поста с прогнозом';   
comment on column parser_log.created_at is 'Дата создания записи';
comment on column parser_log.updated_at is 'Дата обновления записи';
