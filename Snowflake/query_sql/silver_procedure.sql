CREATE
OR REPLACE STREAM AIRLINE.PUBLIC.AIRLINE_BRONZE_STREAM ON TABLE AIRLINE.PUBLIC.AIRLINE_BRONZE;
CREATE OR REPLACE PROCEDURE AIRLINE.SILVER.SP_LOAD_SILVER_LAYER()
    RETURNS STRING
    LANGUAGE SQL
    AS $$
    DECLARE
    passenger INT DEFAULT 0;
    fact_flights INT DEFAULT 0;
    airport INT DEFAULT 0;
    BEGIN
        BEGIN TRANSACTION;
        MERGE INTO AIRLINE.SILVER.PASSENGER_SILVER_LAYER AS target USING(
        SELECT DISTINCT PASSENGER_ID, 
                        FIRST_NAME, 
                        LAST_NAME, 
                        GENDER, AGE, 
                        NATIONALITY 
        FROM AIRLINE.PUBLIC.AIRLINE_BRONZE_STREAM WHERE METADATA$ACTION = 'INSERT') AS source 
        ON target.PASSENGER_ID = source.PASSENGER_ID
        WHEN NOT MATCHED THEN INSERT(PASSENGER_ID, 
                                     FIRST_NAME, 
                                     LAST_NAME, 
                                     GENDER, 
                                     AGE, 
                                     NATIONALITY)
        VALUES(source.PASSENGER_ID, source.FIRST_NAME, source.LAST_NAME, source.GENDER, source.AGE, source.NATIONALITY);
        passenger := SQLROWCOUNT;

       INSERT INTO AIRLINE.SILVER.FLIGHTS_SILVER_LAYER (PASSENGER_ID,
                                                        PILOT_NAME,
                                                        DEPARTURE_DATE,
                                                        ARRIVAL_AIRPORT,
                                                        FLIGHT_STATUS,
                                                        TICKET_TYPE,
                                                        AIRPORT_NAME)                                              
        SELECT 
            PASSENGER_ID,
            PILOT_NAME,
            DEPARTURE_DATE, 
            CASE 
                WHEN ARRIVAL_AIRPORT = '0' OR ARRIVAL_AIRPORT IS NULL THEN 'UNKNOWN'
                ELSE ARRIVAL_AIRPORT 
            END,
            FLIGHT_STATUS,  
            TICKET_TYPE,
            AIRPORT_NAME     
        FROM AIRLINE.PUBLIC.AIRLINE_BRONZE_STREAM 
        WHERE METADATA$ACTION = 'INSERT';
        
        fact_flights := SQLROWCOUNT;
        MERGE INTO AIRLINE.SILVER.AIRPORT_LOCATION_SILVER_LAYER AS target USING(
        SELECT DISTINCT AIRPORT_NAME,
                        AIRPORT_COUNTRY_CODE,
                        REGEXP_REPLACE(COUNTRY_NAME, '([^,]+),\\s*(.*)', '\\2 \\1') AS COUNTRY_NAME,
                        AIRPORT_CONTINENT 
        FROM AIRLINE.PUBLIC.AIRLINE_BRONZE_STREAM WHERE METADATA$ACTION = 'INSERT') AS source
        ON target.AIRPORT_NAME = source.AIRPORT_NAME
        WHEN NOT MATCHED THEN INSERT (AIRPORT_NAME,
                                      AIRPORT_COUNTRY_CODE,
                                      COUNTRY_NAME,
                                      AIRPORT_CONTINENT)
        VALUES(source.AIRPORT_NAME, source.AIRPORT_COUNTRY_CODE, source.COUNTRY_NAME, source.AIRPORT_CONTINENT);
        airport := SQLROWCOUNT;
        
        INSERT INTO AIRLINE.CONTROL.AUDIT_LOG (TRANSACTION_NAME, ROW_AFFECTED, LAYER_TRANSACTION, TARGET_TABLE)
        VALUES ('SP_LOAD_SILVER_LAYERS', :passenger, 'BRONZE_TO_SILVER_SPLIT', 'PASSENGER_SILVER_TABLE'),
               ('SP_LOAD_SILVER_LAYERS', :airport, 'BRONZE_TO_SILVER_SPLIT', 'FLIGHTS_SILVER_TABLE'),
               ('SP_LOAD_SILVER_LAYERS', :fact_flights, 'BRONZE_TO_SILVER_SPLIT', 'AIRPORT_LOCATION_SILVER_TABLE');
     COMMIT;
     RETURN 'Успешно: Пассажиров добавлено ' || COALESCE(:passenger, 0) ||
            ' | Успешно добавлено полётов ' || COALESCE(:fact_flights, 0) ||
            ' | Успешно добавлено аэропортов ' || COALESCE(:airport, 0);

     EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RETURN 'Ошибка: ' || SQLERRM;
END;
$$;