-- Company Information
CREATE TABLE company_info (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    bse_code VARCHAR(20),
    nse_code VARCHAR(20),
    bloomberg_code VARCHAR(20),
    sector VARCHAR(100),
    market_cap_cr DECIMAL(15,2),
    enterprise_value_cr DECIMAL(15,2),
    outstanding_shares_cr DECIMAL(10,2),
    beta DECIMAL(5,2),
    face_value_rs DECIMAL(5,2),
    year_high_price_rs DECIMAL(10,2),
    year_low_price_rs DECIMAL(10,2),
    data_source VARCHAR(255)
);

-- Shareholding Pattern
CREATE TABLE shareholding_pattern (
    company_id INT,
    quarter VARCHAR(20),
    promoter_holding_pct DECIMAL(5,2),
    fii_holding_pct DECIMAL(5,2),
    mf_insti_holding_pct DECIMAL(5,2),
    public_holding_pct DECIMAL(5,2),
    others_holding_pct DECIMAL(5,2),
    data_source VARCHAR(255)
);

-- Price Performance
CREATE TABLE price_performance (
    company_id INT,
    period VARCHAR(20),
    absolute_return_3m_pct DECIMAL(6,2),
    absolute_return_6m_pct DECIMAL(6,2),
    absolute_return_1y_pct DECIMAL(6,2),
    sensex_return_3m_pct DECIMAL(6,2),
    sensex_return_6m_pct DECIMAL(6,2),
    sensex_return_1y_pct DECIMAL(6,2),
    relative_return_3m_pct DECIMAL(6,2),
    relative_return_6m_pct DECIMAL(6,2),
    relative_return_1y_pct DECIMAL(6,2),
    data_source VARCHAR(255)
);

-- Financial Results
CREATE TABLE financial_results (
    financial_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    revenue_cr DECIMAL(15,2),
    yoy_growth_revenue_pct DECIMAL(6,2),
    ebitda_cr DECIMAL(15,2),
    ebitda_margin_pct DECIMAL(6,2),
    net_profit_cr DECIMAL(15,2),
    net_profit_margin_pct DECIMAL(6,2),
    eps_rs DECIMAL(10,2),
    data_source VARCHAR(255)
);

-- Balance Sheet
CREATE TABLE balance_sheet (
    balance_sheet_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    total_assets_cr DECIMAL(15,2),
    total_liabilities_cr DECIMAL(15,2),
    current_assets_cr DECIMAL(15,2),
    cash_cr DECIMAL(15,2),
    inventories_cr DECIMAL(15,2),
    accounts_receivable_cr DECIMAL(15,2),
    accounts_payable_cr DECIMAL(15,2),
    long_term_debt_cr DECIMAL(15,2),
    shareholder_equity_cr DECIMAL(15,2),
    data_source VARCHAR(255)
);

-- Cash Flow
CREATE TABLE cash_flow (
    cash_flow_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    net_cash_from_operations_cr DECIMAL(15,2),
    net_cash_from_investing_cr DECIMAL(15,2),
    net_cash_from_financing_cr DECIMAL(15,2),
    capex_cr DECIMAL(15,2),
    free_cash_flow_cr DECIMAL(15,2),
    data_source VARCHAR(255)
);

-- Key Ratios
CREATE TABLE key_ratios (
    ratio_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    pe_x DECIMAL(10,2),
    pb_x DECIMAL(10,2),
    ev_ebitda_x DECIMAL(10,2),
    roe_pct DECIMAL(6,2),
    roce_pct DECIMAL(6,2),
    dividend_yield_pct DECIMAL(6,2),
    data_source VARCHAR(255)
);

-- Management Discussion
CREATE TABLE management_discussion (
    discussion_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    topic VARCHAR(100),
    discussion_text TEXT,
    data_source VARCHAR(255)
);

-- Recommendations
CREATE TABLE recommendations (
    recommendation_id INT PRIMARY KEY,
    company_id INT,
    rating VARCHAR(50),
    target_price_rs DECIMAL(10,2),
    time_horizon_months INT,
    data_source VARCHAR(255)
); 