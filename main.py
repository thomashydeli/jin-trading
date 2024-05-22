from empyrial import (
    empyrial,
    Engine,
    graph_opt,
) # importing empyrial for portfolio analysis, and backtesting
from datetime import (
    datetime,
    timedelta,
) # importing datetime for controlling period of analysis
from pandas_datareader import data as web
import yfinance as yfin
import argparse
import yagmail
import json
import quantstats as qs
from copy import deepcopy
from fpdf import FPDF
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import utc
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import re
yfin.pdr_override()

def my_job():

    # connecting to database:
    DATABASE_URL=os.getenv('DATABASE_URL')
    DATABASE_URL=re.sub('postgres','postgresql',DATABASE_URL)
    engine=create_engine(DATABASE_URL)

    connection=engine.connect()

    # # Create the parser
    # parser = argparse.ArgumentParser(description="Parsing parameters for stock portfolio rebalance")

    # # Add an argument
    # parser.add_argument('portfolio', type=str, help='Input stock tickers (comma-separated strings)')
    # parser.add_argument('balance', type=float, help='Current balance of your portfolio')
    # parser.add_argument('--benchmark', type=str, default='SPY', help='Benchmark stock ticker')

    # # Parse the arguments
    # args = parser.parse_args()

    # replacing arg parser with os environment variables
    PORTFOLIO = os.getenv('PORTFOLIO', 'MSFT,NVDA,META,BAC,AXP,MCD,KO,AMZN,WMT,CVX,YUM').split(',')
    BENCHMARK = os.getenv('BENCHMARK','SPY')
    # BALANCE = float(os.getenv('BALANCE', '5000'))
    # querying for BALANCE information if table exists
    row_count=connection.execute(
        text("SELECT COUNT(*) FROM transactions WHERE tag <> 'prod';")
    ).fetchall()[0][0]
    # transactions data now created
    if row_count==0:
        BALANCE = float(os.getenv('BALANCE', '5000'))
    else:
        last_transactions=connection.execute(
            text("SELECT * FROM transactions WHERE snapshot=(SELECT MAX(snapshot) FROM transactions) AND tag = 'test';")
        ).fetchall()

    # Split the string into a list
    # PORTFOLIO=args.portfolio.split(',')
    _PORTFOLIO=deepcopy(PORTFOLIO)

    PORTFOLIO_DATA=[
        web.DataReader(
            stock,
            start=str(datetime.now()-timedelta(days=7))[:10],
            end=str(datetime.now())[:10]
        ).reset_index()[['Date','Close']] for stock in PORTFOLIO
    ]

    if row_count != 0:
        price_lookup={s:d.iloc[-1]['Close'] for s, d in zip(PORTFOLIO, PORTFOLIO_DATA)}
        share_lookup={transaction[1]:transaction[2] for transaction in last_transactions}
        BALANCE = 0
        for s in PORTFOLIO:
            BALANCE+=price_lookup[s] * share_lookup[s]

    print(f'parsed portfolio as: {PORTFOLIO}')
    # BENCHMARK=args.benchmark # Using SPY as the benchmark for comparison
    print(f'benchmark chosen as: {BENCHMARK}')
    # BALANCE=args.balance # start with a balance of $10,000


    # getting weights using 3 years of data
    print('working on portfolio rebalancing and backtesting')
    portfolio=Engine(
        start_date=str(datetime.now()-timedelta(days=3*366))[:10],
        benchmark=[BENCHMARK],
        portfolio=PORTFOLIO,
        optimizer='EF',
        max_weights=0.5,
        risk_manager={
            "Stop Loss":-0.2
        },
        rebalance='weekly',
    )
    today_date=str(datetime.now())[:10]
    op=empyrial(
        portfolio,
        report=False,
    )

    # attempt of generating images for embedding into the email body
    _returns=empyrial.returns
    _benchmark=empyrial.benchmark
    qs.plots.returns(_returns, _benchmark, cumulative=True, savefig="retbench.png",show=False);
    qs.plots.yearly_returns(_returns, _benchmark, savefig="y_returns.png",show=False);
    qs.plots.monthly_heatmap(_returns, _benchmark, savefig="heatmap.png",show=False);
    qs.plots.drawdown(_returns, savefig="drawdown.png",show=False);
    qs.plots.drawdowns_periods(_returns, savefig="d_periods.png",show=False);
    qs.plots.rolling_volatility(_returns, savefig="rvol.png",show=False);
    qs.plots.rolling_sharpe(_returns, savefig="rsharpe.png",show=False);
    qs.plots.rolling_beta(_returns, _benchmark, savefig="rbeta.png",show=False);

    CS = [
        "#ff9999",
        "#66b3ff",
        "#99ff99",
        "#ffcc99",
        "#f6c9ff",
        "#a6fff6",
        "#fffeb8",
        "#ffe1d4",
        "#cccdff",
        "#fad6ff",
    ]

    fig1, ax1 = plt.subplots();
    fig1.set_size_inches(7, 7);
    ax1.pie(portfolio.weights, labels=_PORTFOLIO, autopct="%1.1f%%", shadow=False, colors=CS);
    ax1.axis("equal");  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.rcParams["font.size"] = 14;
    plt.savefig("allocation.png");

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("arial", "B", 14)
    pdf.image(
        "https://user-images.githubusercontent.com/61618641/120909011-98f8a180-c670-11eb-8844-2d423ba3fa9c.png",
        x=None,
        y=None,
        w=45,
        h=5,
        type="",
        link="https://github.com/ssantoshp/Empyrial",
    )
    pdf.cell(20, 15, f"Report", ln=1)
    pdf.set_font("arial", size=11)
    pdf.image("allocation.png", x=135, y=0, w=70, h=70, type="", link="")
    pdf.cell(20, 7, f"Start date: " + str(portfolio.start_date), ln=1)
    pdf.cell(20, 7, f"End date: " + str(portfolio.end_date), ln=1)
    y = []
    for x in _returns:
        y.append(x)

    arr = np.array(y)
    # arr
    # returns.index
    my_color = np.where(arr >= 0, "blue", "grey")
    ret = plt.figure(figsize=(30, 8));
    plt.vlines(x=_returns.index, ymin=0, ymax=arr, color=my_color, alpha=0.4);
    plt.title("Returns");
    ret.savefig("ret.png");

    pdf.cell(20, 7, f"", ln=1)
    pdf.cell(20, 7, f"Annual return: " + str(empyrial.CAGR), ln=1)
    pdf.cell(20, 7, f"Cumulative return: " + str(empyrial.CUM), ln=1)
    pdf.cell(20, 7, f"Annual volatility: " + str(empyrial.VOL), ln=1)
    pdf.cell(20, 7, f"Winning day ratio: " + str(empyrial.win_ratio), ln=1)
    pdf.cell(20, 7, f"Sharpe ratio: " + str(empyrial.SR), ln=1)
    pdf.cell(20, 7, f"Calmar ratio: " + str(empyrial.CR), ln=1)
    pdf.cell(20, 7, f"Information ratio: " + str(empyrial.IR), ln=1)
    pdf.cell(20, 7, f"Stability: " + str(empyrial.STABILITY), ln=1)
    pdf.cell(20, 7, f"Max drawdown: " + str(empyrial.MD), ln=1)
    pdf.cell(20, 7, f"Sortino ratio: " + str(empyrial.SOR), ln=1)
    pdf.cell(20, 7, f"Skew: " + str(empyrial.SK), ln=1)
    pdf.cell(20, 7, f"Kurtosis: " + str(empyrial.KU), ln=1)
    pdf.cell(20, 7, f"Tail ratio: " + str(empyrial.TA), ln=1)
    pdf.cell(20, 7, f"Common sense ratio: " + str(empyrial.CSR), ln=1)
    pdf.cell(20, 7, f"Daily value at risk: " + str(empyrial.VAR), ln=1)
    pdf.cell(20, 7, f"Alpha: " + str(empyrial.AL), ln=1)
    pdf.cell(20, 7, f"Beta: " + str(empyrial.BTA), ln=1)

    pdf.image("ret.png", x=-20, y=None, w=250, h=80, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("y_returns.png", x=-20, y=None, w=200, h=100, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("retbench.png", x=None, y=None, w=200, h=100, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("heatmap.png", x=None, y=None, w=200, h=80, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("drawdown.png", x=None, y=None, w=200, h=80, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("d_periods.png", x=None, y=None, w=200, h=80, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("rvol.png", x=None, y=None, w=190, h=80, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("rsharpe.png", x=None, y=None, w=190, h=80, type="", link="")
    pdf.cell(20, 7, f"", ln=1)
    pdf.image("rbeta.png", x=None, y=None, w=190, h=80, type="", link="")

    pdf.output(dest="F", name='test.pdf')
    print("The PDF was generated successfully!")


    print("calculating round-up shares for owning")
    shares=[
        int(
            (BALANCE * weight) / price_data.iloc[-1,1]
        ) for weight, price_data in zip(
            portfolio.weights,PORTFOLIO_DATA
        )
    ] # round the purchase to integer shares

    print("constructing message")
    messaging=f'Today is {str(datetime.now())[:10]}.\nOwn '+', '.join([
        f"{share} shares of {stock}" for stock, share in zip(PORTFOLIO,shares) if share > 0
    ])+f'.\nEstimated annualized return: {empyrial.CAGR}'
    print(f"message constructed as: {messaging}")

    # preserve share for each ticker within PORTFOLIO as a row, and if share=0 just save it
    snapshot=str(datetime.now())[:10]
    final_query=';\n'.join(
        [
            f"INSERT INTO transactions (snapshot, stock, share, price, tag) VALUES ('{snapshot}', '{stock}', {share}, {price_lookup[stock]}, 'test');" for stock, share in zip(PORTFOLIO,shares)
        ]
    )
    connection.execute(text(final_query)) # executing the query
    connection.commit()
    connection.close()

    # Setup the email client
    print("sending out report as an email")
    yag = yagmail.SMTP(os.environ.get('EMAIL_ADDR'), os.environ.get('EMAIL_PWD'))
    # Email content
    contents = [messaging, 'test.pdf']
    # Sending the email
    yag.send('thomashyde23@gmail.com', f'Portfolio Rebalance @ {str(datetime.now())[:10]}', contents)


def main():
    scheduler = BackgroundScheduler(timezone=utc)
    # Schedule the job weekly on Sunday at 23:00 PST
    scheduler.add_job(my_job, 'cron', day_of_week='wed', hour=6, minute=0) # run on Wed 22 PST for testing
    scheduler.add_job(my_job, 'cron', day_of_week='mon', hour=7, minute=0) # run on Sun 23 PST
    # Start the scheduler
    scheduler.start()

    # Keep the script running
    try:
        # Infinite loop to keep the main thread alive
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
