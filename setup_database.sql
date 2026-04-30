-- ============================================================
-- FINANCIAL_AI_HANDSON セットアップスクリプト
-- データベース・ウェアハウス・スキーマの作成
-- GitHubからCSVを取得し、3テーブルを作成
--
-- ※ 複数ユーザーが同一アカウントで実行可能。
--   DB名に CURRENT_USER() をプレフィックスとして付与し、
--   ユーザーごとにリソースを分離します。
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- STEP 1: クロスリージョン推論の有効化、Snowflake Intelligence のオブジェクト作成
-- ============================================================
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
CREATE OR REPLACE SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- ============================================================
-- STEP 2: ウェアハウスの作成（全ユーザー共有）
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Financial AI Handson';

-- ============================================================
-- STEP 3〜12: Snowflake Scriptingブロックで実行
-- ============================================================
DECLARE
    DB_NAME VARCHAR;
BEGIN
    DB_NAME := REPLACE(REPLACE(CURRENT_USER(), '.', '_'), '-', '_') || '_FINANCIAL_AI_HANDSON';

    -- STEP 3: データベースの作成
    EXECUTE IMMEDIATE 'CREATE DATABASE IF NOT EXISTS ' || :DB_NAME;
    EXECUTE IMMEDIATE 'USE DATABASE ' || :DB_NAME;

    -- STEP 4: スキーマの作成
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :DB_NAME || '.RAW COMMENT = ''生データ格納用スキーマ''';
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :DB_NAME || '.ANALYTICS COMMENT = ''分析・AI処理結果格納用スキーマ''';

    -- STEP 5: ステージの作成
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STAGE ' || :DB_NAME || '.RAW.HANDSON_RESOURCES
        DIRECTORY = (ENABLE = TRUE)
        ENCRYPTION = (TYPE = ''SNOWFLAKE_SSE'')
        COMMENT = ''Stage for Financial AI Handson resources''';

    -- STEP 6: GitHub連携 — Git Integrationの作成
    EXECUTE IMMEDIATE 'CREATE OR REPLACE API INTEGRATION financial_handson_git_api_integration
        API_PROVIDER = git_https_api
        API_ALLOWED_PREFIXES = (''https://github.com/'')
        ENABLED = TRUE';

    EXECUTE IMMEDIATE 'CREATE OR REPLACE GIT REPOSITORY ' || :DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON
        API_INTEGRATION = financial_handson_git_api_integration
        ORIGIN = ''https://github.com/sfc-gh-ttakahashi/financial-ai-handson.git''';

    -- STEP 6b: GitリポジトリからNotebookを作成
    EXECUTE IMMEDIATE 'CREATE OR REPLACE NOTEBOOK ' || :DB_NAME || '.RAW.FINANCIAL_AI_HANDSON
        FROM @' || :DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON/branches/main/
        MAIN_FILE = ''FINANCIAL_AI_HANDSON.ipynb''
        QUERY_WAREHOUSE = COMPUTE_WH';

    -- STEP 7: GitHubからCSVをステージにコピー
    EXECUTE IMMEDIATE 'COPY FILES INTO @' || :DB_NAME || '.RAW.HANDSON_RESOURCES/csv/
        FROM @' || :DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON/branches/main/csv/
        PATTERN = ''.*\\.csv$''';

    -- STEP 8: CSVファイルフォーマットの作成
    EXECUTE IMMEDIATE 'CREATE OR REPLACE FILE FORMAT ' || :DB_NAME || '.RAW.CSV_FORMAT
        TYPE = ''CSV''
        FIELD_DELIMITER = '',''
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
        NULL_IF = ('''', ''NULL'')
        ENCODING = ''UTF8''';

    -- STEP 9: 融資商品マスタテーブルの作成とロード
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || :DB_NAME || '.RAW.LOAN_MASTER (
        LOAN_CODE   VARCHAR(20)  NOT NULL COMMENT ''融資商品コード（商品識別コード）'',
        LOAN_NAME   VARCHAR(500) NOT NULL COMMENT ''融資商品名'',
        DEPT_NAME   VARCHAR(100)          COMMENT ''部門名（個人ローン部・法人ローン部・不動産ファイナンス部・中小企業支援部）'',
        DEPT_CODE   VARCHAR(20)           COMMENT ''部門コード''
    ) COMMENT = ''融資商品マスタ（商品コード・商品名・部門情報）''';

    EXECUTE IMMEDIATE 'COPY INTO ' || :DB_NAME || '.RAW.LOAN_MASTER
        (LOAN_CODE, LOAN_NAME, DEPT_NAME, DEPT_CODE)
    FROM @' || :DB_NAME || '.RAW.HANDSON_RESOURCES/csv/product_master.csv
    FILE_FORMAT = (FORMAT_NAME = ''' || :DB_NAME || '.RAW.CSV_FORMAT'')
    ON_ERROR = ''CONTINUE''';

    -- STEP 10: 融資実行データテーブルの作成とロード
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || :DB_NAME || '.ANALYTICS.EXECUTION_DATA (
        EXECUTION_DATE   DATE         NOT NULL COMMENT ''融資実行日'',
        LOAN_CODE        VARCHAR(20)  NOT NULL COMMENT ''融資商品コード'',
        LOAN_NAME        VARCHAR(500)          COMMENT ''融資商品名'',
        DEPT_NAME        VARCHAR(100)          COMMENT ''部門名'',
        EXECUTION_COUNT  NUMBER(10,0)          COMMENT ''融資実行件数（件）'',
        BASE_AMOUNT      NUMBER(15,0)          COMMENT ''基準融資額（円）'',
        TOTAL_AMOUNT     NUMBER(20,0)          COMMENT ''融資実行総額（円）''
    ) COMMENT = ''日次融資実行データ（2022年〜2026年2月）。商品別の実行件数・基準融資額・実行総額を含む。''';

    EXECUTE IMMEDIATE 'COPY INTO ' || :DB_NAME || '.ANALYTICS.EXECUTION_DATA
        (EXECUTION_DATE, LOAN_CODE, LOAN_NAME, DEPT_NAME, EXECUTION_COUNT, BASE_AMOUNT, TOTAL_AMOUNT)
    FROM @' || :DB_NAME || '.RAW.HANDSON_RESOURCES/csv/transaction_data.csv
    FILE_FORMAT = (FORMAT_NAME = ''' || :DB_NAME || '.RAW.CSV_FORMAT'')
    ON_ERROR = ''CONTINUE''';

    -- STEP 11: 融資残高スナップショットテーブルの作成とロード
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || :DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO (
        SNAPSHOT_DATE   DATE         NOT NULL COMMENT ''残高スナップショット日（2026-02-24時点）'',
        LOAN_CODE       VARCHAR(20)  NOT NULL COMMENT ''融資商品コード'',
        LOAN_NAME       VARCHAR(500)          COMMENT ''融資商品名'',
        DEPT_NAME       VARCHAR(100)          COMMENT ''部門名'',
        BALANCE_COUNT   NUMBER(10,0)          COMMENT ''融資残高件数（件）'',
        BASE_AMOUNT     NUMBER(15,0)          COMMENT ''基準融資額（円）'',
        BALANCE_AMOUNT  NUMBER(20,0)          COMMENT ''融資残高総額（円）'',
        CREDIT_STATUS   VARCHAR(20)           COMMENT ''与信ステータス（正常 / 要管理 / 不良）''
    ) COMMENT = ''融資残高スナップショットデータ（2026-02-24時点）。商品別の残高件数・残高金額・与信ステータスを含む。''';

    EXECUTE IMMEDIATE 'COPY INTO ' || :DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO
        (SNAPSHOT_DATE, LOAN_CODE, LOAN_NAME, DEPT_NAME, BALANCE_COUNT, BASE_AMOUNT, BALANCE_AMOUNT, CREDIT_STATUS)
    FROM @' || :DB_NAME || '.RAW.HANDSON_RESOURCES/csv/portfolio_data.csv
    FILE_FORMAT = (FORMAT_NAME = ''' || :DB_NAME || '.RAW.CSV_FORMAT'')
    ON_ERROR = ''CONTINUE''';

    -- STEP 12: 完了メッセージ
    RETURN :DB_NAME || ' のセットアップが完了しました';
END;