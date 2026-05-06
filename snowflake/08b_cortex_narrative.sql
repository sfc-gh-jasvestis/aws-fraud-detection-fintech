-- ============================================================================
-- 08b_cortex_narrative.sql
-- ----------------------------------------------------------------------------
-- DEPLOYED VERSION (2026-05-06): Cortex-only SP_GENERATE_CASE_NARRATIVE.
--
-- This supersedes the Bedrock-EAI Python version in 08_bedrock.sql when no
-- external access integration is configured. Uses SNOWFLAKE.CORTEX.COMPLETE
-- with claude-sonnet-4-5 and returns a parsed VARIANT with the keys the
-- Streamlit UI expects: case_summary, sar_narrative, risk_indicators,
-- recommended_action, next_steps.
-- ============================================================================

CREATE OR REPLACE PROCEDURE CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE(P_CASE_ID VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Generates SAR narrative + structured fields using Cortex COMPLETE (claude-sonnet-4-5)'
EXECUTE AS OWNER
AS
$$
DECLARE
    case_data VARIANT;
    prompt    STRING;
    narrative STRING;
    cleaned   STRING;
    parsed    VARIANT;
    sar_text  STRING;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'case_id', CASE_ID,
        'case_ref', CASE_REF,
        'entity_id', ENTITY_ID,
        'state', STATE,
        'priority', PRIORITY,
        'title', TITLE,
        'summary', SUMMARY,
        'alert_types', ALERT_TYPES,
        'composite_severity', COMPOSITE_SEVERITY,
        'peak_ml_probability', PEAK_ML_PROBABILITY,
        'created_at', CREATED_AT::STRING
    )
    INTO :case_data
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES
    WHERE CASE_ID = :P_CASE_ID;

    IF (case_data IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'Case not found: ' || :P_CASE_ID);
    END IF;

    prompt := 'You are a senior financial-crime investigator at a digital-asset exchange. ' ||
              'Review this case and respond with ONLY a JSON object (no markdown, no code fences) with these exact keys: ' ||
              '{"case_summary": "3-5 sentence executive summary", ' ||
              '"sar_narrative": "Formal SAR narrative in FinCEN format, 2-3 paragraphs", ' ||
              '"risk_indicators": ["bullet 1","bullet 2","bullet 3"], ' ||
              '"recommended_action": "FILE_SAR | FREEZE_ACCOUNT | MONITOR | DISMISS", ' ||
              '"next_steps": ["step 1","step 2","step 3"]}. ' ||
              'Case data: ' || case_data::STRING;

    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-5', :prompt) INTO :narrative;

    cleaned := TRIM(:narrative);
    IF (cleaned LIKE '```%') THEN
        cleaned := REGEXP_REPLACE(cleaned, '^```(json)?\\s*', '');
        cleaned := REGEXP_REPLACE(cleaned, '\\s*```$', '');
    END IF;

    SELECT TRY_PARSE_JSON(:cleaned) INTO :parsed;

    IF (parsed IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'Failed to parse Cortex JSON', 'raw', :cleaned);
    END IF;

    SELECT :parsed:sar_narrative::STRING INTO :sar_text;

    UPDATE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
    SET SAR_NARRATIVE = :sar_text,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE CASE_ID = :P_CASE_ID;

    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.CASE_EVENTS (CASE_ID, EVENT_TYPE, EVENT_DATA, PERFORMED_BY)
    SELECT :P_CASE_ID, 'NARRATIVE_GENERATED',
           OBJECT_CONSTRUCT('source','SP_GENERATE_CASE_NARRATIVE','length',LENGTH(:cleaned)),
           CURRENT_USER();

    RETURN :parsed;
END;
$$;
