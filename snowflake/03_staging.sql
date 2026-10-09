-- Validate the producer contract before building downstream objects.
USE DATABASE IDENTIFIER($DEMO_DB);
USE SCHEMA RAW;
USE WAREHOUSE IDENTIFIER($DEMO_WH);

EXECUTE IMMEDIATE $$
DECLARE
  violations INTEGER;
  invalid_source EXCEPTION (-20001, 'Synthetic source failed grain or measure validation');
BEGIN
  SELECT COUNT(*) INTO :violations FROM (
    SELECT ENTITY_ID, EVENT_DATE
    FROM RAW.ACCOUNT_DAILY
    GROUP BY ENTITY_ID, EVENT_DATE HAVING COUNT(*) <> 1
    UNION ALL
    SELECT observation.ENTITY_ID, observation.EVENT_DATE
    FROM RAW.ACCOUNT_DAILY observation
    LEFT JOIN RAW.ACCOUNTS account ON account.ID = observation.ENTITY_ID
    WHERE account.ID IS NULL OR observation.TRADE_COUNT < 0
       OR observation.NOTIONAL_USD < 0
       OR observation.CONFIRMED_COUNT < 0 OR observation.CONFIRMED_COUNT > observation.ALERT_COUNT
       OR observation.SAR_FILED > observation.CONFIRMED_COUNT
       OR observation.REVIEW_COMPLETED > observation.REVIEW_DUE
  );
  IF (violations > 0) THEN
    RAISE invalid_source;
  END IF;
END;
$$;
