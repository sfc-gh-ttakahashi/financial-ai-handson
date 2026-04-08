# AGENTS.md - financial-ai-handson

## プロジェクト概要

銀行の融資ポートフォリオ（個人・法人・不動産・中小企業向け融資）を題材にした Snowflake AI ハンズオンワークショップ。
生データの取り込みから AI エージェント構築・評価までを一貫して体験する約80分の教材。

## アーキテクチャ

```
CSV (product_master / transaction_data / portfolio_data)
  → Snowflake Tables (RAW / ANALYTICS スキーマ)
    → AI_CLASSIFY / AI_COMPLETE による分類・説明生成 (ANALYTICS.AI_LOAN_MASTER)
    → Marketplace マクロ経済データ結合 (ANALYTICS.MACRO_INDICATORS)
    → Cortex Search Service (融資商品検索)
    → Semantic View (Cortex Analyst)
    → Cortex Agent (Analyst + Search + Web Search, claude-4-sonnet)
    → [オプション] Agent 評価 (answer_correctness / logical_consistency)
```

## ディレクトリ構造

```
financial-ai-handson/
├── csv/
│   ├── product_master.csv         # 融資商品マスタ（商品コード・商品名・部門、55商品）
│   ├── transaction_data.csv       # 日次融資実行データ（2022-01〜2026-02、83,380行）
│   └── portfolio_data.csv         # 融資残高スナップショット（2026-02-24時点）
├── FINANCIAL_AI_HANDSON.ipynb     # メイン Snowflake Notebook（全8ステップ）
├── setup_database.sql             # 環境セットアップ SQL
├── eval_config.yaml               # Cortex Agent 評価設定（Step 7 用）
└── AGENTS.md                      # このファイル
```

## Snowflake 環境

- **データベース**: `FINANCIAL_AI_HANDSON`
- **スキーマ**: `RAW`（生データ）/ `ANALYTICS`（分析用）
- **ウェアハウス**: `COMPUTE_WH`（SMALL）
- **クロスリージョン推論**: `CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'`

## 主要テーブル

| テーブル | スキーマ | 説明 |
|---------|---------|------|
| `LOAN_MASTER` | RAW | 融資商品マスタ（LOAN_CODE, LOAN_NAME, DEPT_NAME, DEPT_CODE） |
| `AI_LOAN_MASTER` | ANALYTICS | AI 拡張済み融資商品マスタ（CATEGORY, PRODUCT_CAPTION を AI で自動付与） |
| `EXECUTION_DATA` | ANALYTICS | 日次融資実行データ（実行件数・基準融資額・実行総額） |
| `LOAN_PORTFOLIO` | ANALYTICS | 融資残高スナップショット（残高件数・残高金額・与信ステータス：正常/要管理/不良） |
| `MACRO_INDICATORS` | ANALYTICS | 日次マクロ経済指標（Fruit Stand / FRED Marketplace 由来、政策金利・長期金利・為替・日経平均） |

## 使用する Snowflake AI 機能

| ステップ | 機能 | 用途 |
|:-------:|------|------|
| 1 | AI_CLASSIFY / AI_COMPLETE | 融資商品カテゴリの自動分類、商品キャプションの生成 |
| 2 | Snowflake Marketplace | Fruit Stand 提供 FRED データの取り込み・変換（10万以上のシリーズ、30日間無料トライアル） |
| 3 | Cortex Search Service | snowflake-arctic-embed-l-v2.0 による融資商品のセマンティック検索 |
| 4 | Semantic View (Cortex Analyst) | 3テーブル結合のセマンティックビュー定義（BANKING_ANALYST） |
| 5 | Cortex Agent | Analyst + Search + Web Search のオーケストレーション（claude-4-sonnet） |
| 6 | Cortex Agent UI | Snowsight 上でエージェントを対話的に操作 |
| 7 | Cortex Agent Evaluations | answer_correctness / logical_consistency 評価【オプション】 |
| 8 | Resource Budgets | タグベースの AI コスト管理 |

## 融資商品カテゴリ（AI_CLASSIFY で分類）

| カテゴリ | 対象商品の例 |
|---------|------------|
| 住宅・不動産ローン | 住宅ローン変動型（35年/25年）、フラット35、リフォームローン、居住用不動産担保ローン |
| 自動車・消費者ローン | マイカーローン（新車/中古車）、教育ローン、カードローン、医療・介護ローン、ブライダルローン |
| 事業・設備資金ローン | 短期運転資金（手形貸付/証書貸付）、長期設備資金ローン、IT・DX投資ローン、環境・脱炭素設備ローン |
| 不動産投資ローン | 収益物件取得ローン（区分/一棟）、商業施設開発ローン、物流施設開発ローン、不動産証券化ブリッジローン |
| 創業・事業承継ローン | 創業融資、成長支援融資、事業承継融資、地方創生ローン、再生可能エネルギー事業ローン |

## 部門一覧

| 部門名 | 部門コード | 商品数 |
|--------|----------|--------|
| 個人ローン部 | KOJIN01 | 14 |
| 法人ローン部 | HOJIN01 | 14 |
| 不動産ファイナンス部 | FUDOSAN01 | 14 |
| 中小企業支援部 | SME01 | 13 |

## セマンティックビュー テーブルリレーション

```
                  LOAN_CODE                  EXECUTION_DATE = MARKET_DATE
EXECUTION_DATA ──────────────────▶ PORTFOLIO  EXECUTION_DATA ──────────────▶ MACRO
（日次融資実行）                  （残高スナップ）  （日次融資実行）              （マクロ経済）
```

## データの特徴・季節性

| 商品・カテゴリ | パターン |
|--------------|---------|
| 住宅ローン系 | 3月・9月ピーク（引越しシーズン）、2024年利上げ後は変動型↓・固定型↑ |
| 法人ローン（運転資金・手形） | 四半期末（3月・6月・9月・12月）に実行集中 |
| IT・DX投資ローン / 再生可能エネルギー事業ローン | 年々増加トレンド |
| コロナ回復支援ローン | 2022年高水準→年々減少（与信ステータス：不良） |
| 教育ローン | 3〜4月（入学シーズン）に急増 |

## マクロ経済指標（Fruit Stand / FRED から取得）

**FRED - Federal Reserve Economic Data Series Observations**（Fruit Stand 提供）  
10万以上の FRED シリーズの完全な履歴データ。30日間の無料トライアルで全シリーズにフルアクセス可能。

| カラム | FRED Series ID | 説明 |
|--------|--------------|------|
| `POLICY_RATE` | `IRSTCI01JPM156N` | 日銀の無担保コール翌日物金利（%） |
| `LONG_RATE_10Y` | `IRLTLT01JPM156N` | 日本10年国債利回り（%） |
| `JPY_USD` | `DEXJPUS` | ドル円為替レート（円/ドル） |
| `NIKKEI_AVG` | `NIKKEI225` | 日経225株価指数（円） |

> Marketplace からデータベース名 `FRED__FEDERAL_RESERVE_ECONOMIC_DATA_SERIES_OBSERVATIONS` で取得。  
> 系列一覧ビュー: `FRED.FRED_SERIES_VW`  
> 観測値ビュー: `FRED.FRED_SERIES_OBSERVATIONS_VW`（カラム: `DATE`, `SERIES_ID`, `VALUE`）

## コーディング規約

- SQL は Snowflake SQL で記述する
- カラム名・テーブル名は snake_case を使用する
- コメント・ドキュメントは日本語で記述する
- テーブルやビューには必ず COMMENT を付与する
- 集計クエリでは CTE（WITH句）を使って段階的に組み立てる
- 数値は ROUND(value, 2) で小数2桁に丸める
- デフォルトの集計期間は直近1年間とする
- ランキングには QUALIFY + ROW_NUMBER を使用する

## ハンズオン受講者への注意

- Step 2（Marketplace）は事前に **FRED - Federal Reserve Economic Data Series Observations**（Fruit Stand 提供）のデータ取得が必要（30日間無料トライアルあり）
- Step 7（Agent Evaluations）はトライアルアカウントでは実行不可（有償契約アカウントが必要）
- `eval_config.yaml` は Step 7 で使用する評価設定ファイル（ステージへのアップロードが必要）
- FRED データベース名はアカウントによって異なる場合がある（デフォルト: `FRED__FEDERAL_RESERVE_ECONOMIC_DATA_SERIES_OBSERVATIONS`）
