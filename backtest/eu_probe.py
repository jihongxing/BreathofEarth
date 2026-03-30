"""探测欧洲市场 ETF 数据可用性"""
import yfinance as yf

tickers = {
    "EZU": "iShares MSCI Eurozone",
    "VGK": "Vanguard FTSE Europe",
    "BWX": "SPDR Intl Treasury Bond",
    "IGOV": "iShares Intl Treasury Bond",
    "BNDX": "Vanguard Total Intl Bond",
}

for t, name in tickers.items():
    df = yf.download(t, start="2005-01-01", end="2025-12-31", auto_adjust=False, progress=False)
    if not df.empty:
        if isinstance(df.columns, __import__('pandas').MultiIndex):
            adj = df["Adj Close"].iloc[:, 0] if isinstance(df["Adj Close"], __import__('pandas').DataFrame) else df["Adj Close"]
        else:
            adj = df["Adj Close"]
        print(f"✓ {t:6} ({name:35}): {len(adj):>5} rows | {adj.index[0].date()} ~ {adj.index[-1].date()} | ${adj.iloc[0]:.2f} → ${adj.iloc[-1]:.2f}")
    else:
        print(f"✗ {t:6} ({name:35}): no data")
