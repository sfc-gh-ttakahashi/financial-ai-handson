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
-- STEP 1: クロスリージョン推論を有効化
-- ============================================================
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

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
-- STEP 3: データベースの作成（ユーザー名プレフィックス付き）
-- ============================================================
-- ユーザー名をサニタイズしてDB名を生成（ドットやハイフンをアンダースコアに変換）
SET DB_NAME = (SELECT REPLACE(REPLACE(CURRENT_USER(), '.', '_'), '-', '_') || '_FINANCIAL_AI_HANDSON');
EXECUTE IMMEDIATE (SELECT 'CREATE DATABASE IF NOT EXISTS ' || $DB_NAME);
EXECUTE IMMEDIATE (SELECT 'USE DATABASE ' || $DB_NAME);

-- ============================================================
-- STEP 4: スキーマの作成
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'CREATE SCHEMA IF NOT EXISTS ' || $DB_NAME || '.RAW COMMENT = ''生データ格納用スキーマ''');
EXECUTE IMMEDIATE (SELECT 'CREATE SCHEMA IF NOT EXISTS ' || $DB_NAME || '.ANALYTICS COMMENT = ''分析・AI処理結果格納用スキーマ''');

EXECUTE IMMEDIATE (SELECT 'SHOW SCHEMAS IN DATABASE ' || $DB_NAME);

-- ============================================================
-- STEP 5: ステージの作成
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'USE SCHEMA ' || $DB_NAME || '.RAW');

EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE STAGE ' || $DB_NAME || '.RAW.HANDSON_RESOURCES
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = ''SNOWFLAKE_SSE'')
    COMMENT = ''Stage for Financial AI Handson resources''');

-- ============================================================
-- STEP 6: GitHub連携 — Git Integrationの作成（全ユーザー共有）
-- ============================================================
CREATE OR REPLACE API INTEGRATION financial_handson_git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED = TRUE;

EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE GIT REPOSITORY ' || $DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON
    API_INTEGRATION = financial_handson_git_api_integration
    ORIGIN = ''https://github.com/sfc-gh-ttakahashi/financial-ai-handson.git''');

-- ============================================================
-- STEP 6b: GitリポジトリからNotebookを作成
--   GitHubリポジトリ上の FINANCIAL_AI_HANDSON.ipynb を
--   Snowflake Notebook として取り込みます。
--   取り込み後、Snowsight の「Notebooks」からアクセスできます。
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE NOTEBOOK ' || $DB_NAME || '.RAW.FINANCIAL_AI_HANDSON
    FROM @' || $DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON/branches/main/
    MAIN_FILE = ''FINANCIAL_AI_HANDSON.ipynb''
    QUERY_WAREHOUSE = COMPUTE_WH');

-- リポジトリの内容確認
EXECUTE IMMEDIATE (SELECT 'LIST @' || $DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON/branches/main');

-- ============================================================
-- STEP 7: GitHubからCSVをステージにコピー
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'COPY FILES INTO @' || $DB_NAME || '.RAW.HANDSON_RESOURCES/csv/
    FROM @' || $DB_NAME || '.RAW.GIT_FINANCIAL_HANDSON/branches/main/csv/
    PATTERN = ''.*\\.csv$''');

-- コピーされたファイルの確認
EXECUTE IMMEDIATE (SELECT 'LIST @' || $DB_NAME || '.RAW.HANDSON_RESOURCES/csv/');

-- ============================================================
-- STEP 8: CSVファイルフォーマットの作成
-- ============================================================
USE WAREHOUSE COMPUTE_WH;

EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE FILE FORMAT ' || $DB_NAME || '.RAW.CSV_FORMAT
    TYPE = ''CSV''
    FIELD_DELIMITER = '',''
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
    NULL_IF = ('''', ''NULL'')
    ENCODING = ''UTF8''');

-- ============================================================
-- STEP 9: 融資商品マスタテーブルの作成とロード
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE TABLE ' || $DB_NAME || '.RAW.LOAN_MASTER (
    LOAN_CODE   VARCHAR(20)  NOT NULL COMMENT ''融資商品コード（商品識別コード）'',
    LOAN_NAME   VARCHAR(500) NOT NULL COMMENT ''融資商品名'',
    DEPT_NAME   VARCHAR(100)          COMMENT ''部門名（個人ローン部・法人ローン部・不動産ファイナンス部・中小企業支援部）'',
    DEPT_CODE   VARCHAR(20)           COMMENT ''部門コード''
) COMMENT = ''融資商品マスタ（商品コード・商品名・部門情報）''');

EXECUTE IMMEDIATE (SELECT 'COPY INTO ' || $DB_NAME || '.RAW.LOAN_MASTER
    (LOAN_CODE, LOAN_NAME, DEPT_NAME, DEPT_CODE)
FROM @' || $DB_NAME || '.RAW.HANDSON_RESOURCES/csv/product_master.csv
FILE_FORMAT = (FORMAT_NAME = ''' || $DB_NAME || '.RAW.CSV_FORMAT'')
ON_ERROR = ''CONTINUE''');

EXECUTE IMMEDIATE (SELECT 'SELECT COUNT(*) AS TOTAL_RECORDS FROM ' || $DB_NAME || '.RAW.LOAN_MASTER');
EXECUTE IMMEDIATE (SELECT 'SELECT * FROM ' || $DB_NAME || '.RAW.LOAN_MASTER LIMIT 10');

-- ============================================================
-- STEP 10: 融資実行データ（日次取引データ）テーブルの作成とロード
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE TABLE ' || $DB_NAME || '.ANALYTICS.EXECUTION_DATA (
    EXECUTION_DATE   DATE         NOT NULL COMMENT ''融資実行日'',
    LOAN_CODE        VARCHAR(20)  NOT NULL COMMENT ''融資商品コード'',
    LOAN_NAME        VARCHAR(500)          COMMENT ''融資商品名'',
    DEPT_NAME        VARCHAR(100)          COMMENT ''部門名'',
    EXECUTION_COUNT  NUMBER(10,0)          COMMENT ''融資実行件数（件）'',
    BASE_AMOUNT      NUMBER(15,0)          COMMENT ''基準融資額（円）'',
    TOTAL_AMOUNT     NUMBER(20,0)          COMMENT ''融資実行総額（円）''
) COMMENT = ''日次融資実行データ（2022年〜2026年2月）。商品別の実行件数・基準融資額・実行総額を含む。''');

EXECUTE IMMEDIATE (SELECT 'COPY INTO ' || $DB_NAME || '.ANALYTICS.EXECUTION_DATA
    (EXECUTION_DATE, LOAN_CODE, LOAN_NAME, DEPT_NAME, EXECUTION_COUNT, BASE_AMOUNT, TOTAL_AMOUNT)
FROM @' || $DB_NAME || '.RAW.HANDSON_RESOURCES/csv/transaction_data.csv
FILE_FORMAT = (FORMAT_NAME = ''' || $DB_NAME || '.RAW.CSV_FORMAT'')
ON_ERROR = ''CONTINUE''');

EXECUTE IMMEDIATE (SELECT 'SELECT COUNT(*) AS TOTAL_RECORDS FROM ' || $DB_NAME || '.ANALYTICS.EXECUTION_DATA');
EXECUTE IMMEDIATE (SELECT 'SELECT * FROM ' || $DB_NAME || '.ANALYTICS.EXECUTION_DATA LIMIT 10');

-- ============================================================
-- STEP 11: 融資残高スナップショットテーブルの作成とロード
-- ============================================================
EXECUTE IMMEDIATE (SELECT 'CREATE OR REPLACE TABLE ' || $DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO (
    SNAPSHOT_DATE   DATE         NOT NULL COMMENT ''残高スナップショット日（2026-02-24時点）'',
    LOAN_CODE       VARCHAR(20)  NOT NULL COMMENT ''融資商品コード'',
    LOAN_NAME       VARCHAR(500)          COMMENT ''融資商品名'',
    DEPT_NAME       VARCHAR(100)          COMMENT ''部門名'',
    BALANCE_COUNT   NUMBER(10,0)          COMMENT ''融資残高件数（件）'',
    BASE_AMOUNT     NUMBER(15,0)          COMMENT ''基準融資額（円）'',
    BALANCE_AMOUNT  NUMBER(20,0)          COMMENT ''融資残高総額（円）'',
    CREDIT_STATUS   VARCHAR(20)           COMMENT ''与信ステータス（正常 / 要管理 / 不良）''
) COMMENT = ''融資残高スナップショットデータ（2026-02-24時点）。商品別の残高件数・残高金額・与信ステータスを含む。''');

EXECUTE IMMEDIATE (SELECT 'COPY INTO ' || $DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO
    (SNAPSHOT_DATE, LOAN_CODE, LOAN_NAME, DEPT_NAME, BALANCE_COUNT, BASE_AMOUNT, BALANCE_AMOUNT, CREDIT_STATUS)
FROM @' || $DB_NAME || '.RAW.HANDSON_RESOURCES/csv/portfolio_data.csv
FILE_FORMAT = (FORMAT_NAME = ''' || $DB_NAME || '.RAW.CSV_FORMAT'')
ON_ERROR = ''CONTINUE''');

EXECUTE IMMEDIATE (SELECT 'SELECT COUNT(*) AS TOTAL_RECORDS FROM ' || $DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO');
EXECUTE IMMEDIATE (SELECT 'SELECT * FROM ' || $DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO LIMIT 10');

-- ============================================================
-- STEP 12: データ確認サマリ
-- ============================================================
EXECUTE IMMEDIATE (SELECT
'SELECT ''LOAN_MASTER''    AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ' || $DB_NAME || '.RAW.LOAN_MASTER
UNION ALL
SELECT ''EXECUTION_DATA'',               COUNT(*)              FROM ' || $DB_NAME || '.ANALYTICS.EXECUTION_DATA
UNION ALL
SELECT ''LOAN_PORTFOLIO'',               COUNT(*)              FROM ' || $DB_NAME || '.ANALYTICS.LOAN_PORTFOLIO
ORDER BY TABLE_NAME');

-- ============================================================
-- セットアップ完了
-- DB名の確認: 以下で自分のDB名を確認できます
-- ============================================================
SELECT $DB_NAME AS YOUR_DATABASE_NAME;