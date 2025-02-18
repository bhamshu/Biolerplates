-- =====================================================================
-- SCHEMA DEFINITION (DDL)
-- =====================================================================

-- 1. COMPANY_INFO TABLE
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

-- 2. SHAREHOLDING_PATTERN TABLE
CREATE TABLE shareholding_pattern (
    company_id INT,
    quarter VARCHAR(20),
    promoter_holding_pct DECIMAL(5,2),
    fii_holding_pct DECIMAL(5,2),
    mf_insti_holding_pct DECIMAL(5,2),
    public_holding_pct DECIMAL(5,2),
    others_holding_pct DECIMAL(5,2),
    data_source VARCHAR(255),
    PRIMARY KEY (company_id, quarter),
    CONSTRAINT fk_shp_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 3. PRICE_PERFORMANCE TABLE
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
    data_source VARCHAR(255),
    PRIMARY KEY (company_id, period),
    CONSTRAINT fk_pp_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 4. FINANCIAL_RESULTS TABLE
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
    data_source VARCHAR(255),
    CONSTRAINT fk_fr_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 5. BALANCE_SHEET TABLE
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
    data_source VARCHAR(255),
    CONSTRAINT fk_bs_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 6. CASH_FLOW TABLE
CREATE TABLE cash_flow (
    cash_flow_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    net_cash_from_operations_cr DECIMAL(15,2),
    net_cash_from_investing_cr DECIMAL(15,2),
    net_cash_from_financing_cr DECIMAL(15,2),
    capex_cr DECIMAL(15,2),
    free_cash_flow_cr DECIMAL(15,2),
    data_source VARCHAR(255),
    CONSTRAINT fk_cf_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 7. KEY_RATIOS TABLE
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
    data_source VARCHAR(255),
    CONSTRAINT fk_kr_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 8. OUTLOOK_OR_MANAGEMENT_DISCUSSION TABLE
CREATE TABLE outlook_or_management_discussion (
    discussion_id INT PRIMARY KEY,
    company_id INT,
    fiscal_period VARCHAR(20),
    topic VARCHAR(100),
    discussion_text TEXT,
    data_source VARCHAR(255),
    CONSTRAINT fk_omd_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- 9. RECOMMENDATIONS_OR_TARGETS TABLE
CREATE TABLE recommendations_or_targets (
    recommendation_id INT PRIMARY KEY,
    company_id INT,
    rating VARCHAR(50),
    target_price_rs DECIMAL(10,2),
    time_horizon_months INT,
    data_source VARCHAR(255),
    CONSTRAINT fk_rt_company
        FOREIGN KEY (company_id)
        REFERENCES company_info(company_id)
);

-- =====================================================================
-- SAMPLE INSERT STATEMENTS
-- =====================================================================

-- 1. COMPANY_INFO data
INSERT INTO company_info (company_id, company_name, bse_code, nse_code, bloomberg_code, sector,
    market_cap_cr, enterprise_value_cr, outstanding_shares_cr, beta, face_value_rs,
    year_high_price_rs, year_low_price_rs, data_source)
VALUES
(1, 'Sun Pharmaceutical Industries Limited', '524715', 'SUNPHARMA', 'SUNP:IN', 'Pharmaceuticals',
 351905, 346823, 239.9, 0.5, 1, 1639, 978, 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 'ABB India Limited', '500002', 'ABB', 'ABB:IN', 'Capital Goods',
 171648, 166555, 21.2, 1.2, 2, 8818, 3848, 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 'Oil & Natural Gas Corporation Ltd.', '500312', 'ONGC', 'ONGC:IN', 'Oil, Gas & Consumable Fuels',
 344196, 350289, 1258.0, 1.6, 5, 293, 155, 'SP20241306102605493ONGC.pdf');

-- 2. SHAREHOLDING_PATTERN data
INSERT INTO shareholding_pattern (company_id, quarter, promoter_holding_pct, fii_holding_pct,
    mf_insti_holding_pct, public_holding_pct, others_holding_pct, data_source)
VALUES
(1, 'Q4FY24', 54.5, 17.7, 18.8, 8.5, 0.5, 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 'Q1CY24', 75.0, 11.9, 6.0, 6.9, 0.2, 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 'Q4FY24', 58.9, 8.9, 18.9, 2.7, 10.6, 'SP20241306102605493ONGC.pdf');

-- 3. PRICE_PERFORMANCE data
INSERT INTO price_performance (company_id, period, absolute_return_3m_pct, absolute_return_6m_pct,
    absolute_return_1y_pct, sensex_return_3m_pct, sensex_return_6m_pct, sensex_return_1y_pct,
    relative_return_3m_pct, relative_return_6m_pct, relative_return_1y_pct, data_source)
VALUES
(1, 'Q4FY24', -7.3, 22.6, 51.1, 3.1, 14.3, 20.6, -10.4, 8.3, 30.5,
 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 'Q1CY24', 39.0, 69.1, 95.2, -0.3, 4.8, 17.0, 39.3, 64.3, 78.2,
 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 'Q4FY24', -6.8, 32.3, 68.0, 3.2, 9.5, 22.1, -10.0, 22.8, 45.8,
 'SP20241306102605493ONGC.pdf');

-- 4. FINANCIAL_RESULTS data
INSERT INTO financial_results (financial_id, company_id, fiscal_period, revenue_cr, yoy_growth_revenue_pct,
    ebitda_cr, ebitda_margin_pct, net_profit_cr, net_profit_margin_pct, eps_rs, data_source)
VALUES
(1, 1, 'Q4FY24', 11813, 10.1, 3092, 26.2, 2756, (2756 / 11813)*100, 11.5,
 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 2, 'Q1CY24', 3080, 27.8, 565, 18.3, 459, (459 / 3080)*100, 21.7,
 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 3, 'Q4FY24', 34637, -4.6, 19571, 56.5, 9869, (9869 / 34637)*100, 7.8,
 'SP20241306102605493ONGC.pdf');

-- 5. BALANCE_SHEET data
INSERT INTO balance_sheet (balance_sheet_id, company_id, fiscal_period, total_assets_cr, total_liabilities_cr,
    current_assets_cr, cash_cr, inventories_cr, accounts_receivable_cr, accounts_payable_cr,
    long_term_debt_cr, shareholder_equity_cr, data_source)
VALUES
(1, 1, 'FY24', 85463, 21796, 43475, 10521, 9868, 11249, 14140, 2846, 63667,
 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 2, 'CY23', 11001, 5056, 9547, 4816, 1561, 2544, 5014, 37, 5945,
 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 3, 'FY24', 446021, 140044, 65743, 35, 10712, 11410, 39448, 6109, 305977,
 'SP20241306102605493ONGC.pdf');

-- 6. CASH_FLOW data
INSERT INTO cash_flow (cash_flow_id, company_id, fiscal_period, net_cash_from_operations_cr,
    net_cash_from_investing_cr, net_cash_from_financing_cr, capex_cr, free_cash_flow_cr, data_source)
VALUES
(1, 1, 'FY24', 12135, -690, -6710, 2171, (12135 - 2171), 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 2, 'CY23', 1352, -3352, -269, 183, (1352 - 183), 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 3, 'FY24', 65336, -42694, -22685, 37667, (65336 - 37667), 'SP20241306102605493ONGC.pdf');

-- 7. KEY_RATIOS data
INSERT INTO key_ratios (ratio_id, company_id, fiscal_period, pe_x, pb_x, ev_ebitda_x, roe_pct,
    roce_pct, dividend_yield_pct, data_source)
VALUES
(1, 1, 'FY24', 38.7, 6.1, 29.6, 15.1, NULL, 0.9, 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 2, 'CY24E', 101.2, 23.5, 81.8, 23.2, NULL, 0.4, 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 3, 'FY24', 8.3, 1.1, 4.4, 13.2, NULL, 3.7, 'SP20241306102605493ONGC.pdf');

-- 8. OUTLOOK_OR_MANAGEMENT_DISCUSSION data
INSERT INTO outlook_or_management_discussion (discussion_id, company_id, fiscal_period, topic,
    discussion_text, data_source)
VALUES
(1, 1, 'Q4FY24', 'Guidance',
 'Sun Pharma anticipates high single-digit revenue growth for FY25. The global specialty business is expected to continue expanding. R&D investments to be 8–10% of sales.',
 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 2, 'Q1CY24', 'Order Book & Efficiency',
 'ABB expects increased use of high efficiency power drives. Growing data center expansions, strong backlog execution, and cost optimisation will drive future growth.',
 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 3, 'Q4FY24', 'Production Outlook',
 'ONGC plans to ramp up output from KG 98/2 to 45,000 barrels by Q4FY25 and increase gas production to 10 million cubic meters/day. Aims to reach ~47 mmtoe by FY27.',
 'SP20241306102605493ONGC.pdf');

-- 9. RECOMMENDATIONS_OR_TARGETS data
INSERT INTO recommendations_or_targets (recommendation_id, company_id, rating, target_price_rs,
    time_horizon_months, data_source)
VALUES
(1, 1, 'HOLD', 1655, 12, 'SP20241006115717120Sun PharmaQ4.pdf'),
(2, 2, 'HOLD', 8820, 12, 'SP20241206120345517ABB India Limited_20240611 PDF.pdf'),
(3, 3, 'BUY', 327, 12, 'SP20241306102605493ONGC.pdf');

-- =====================================================================
-- END OF SCHEMA + SAMPLE DATA
-- =====================================================================
