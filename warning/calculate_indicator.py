import pandas as pd
import pandas_ta as ta
import re


def get_latest_data_point(
    df: pd.DataFrame, field: str | None, indicator: str | None, period: int | None
):

    match indicator:
        case None:
            df = df[[field]]
            df.columns = ["value"]

        case "ma":
            df["value"] = ta.sma(df[field], length=period)
            df = df[["value"]]

        case "ema":
            df["value"] = ta.ema(df[field], length=period)
            df = df[["value"]]

        case "stoch_k":
            df["STOCH_k"] = ta.stoch(df["high"], df["low"], df["close"]).iloc[:, 0]
            df = df[["STOCH_k"]].rename(columns={"STOCH_k": "value"})

        case "stoch_d":
            df["STOCH_d"] = ta.stoch(df["high"], df["low"], df["close"]).iloc[:, 1]
            df = df[["STOCH_d"]].rename(columns={"STOCH_d": "value"})

        case "rsi":
            df["value"] = ta.rsi(df["close"], length=period)
            df = df[["value"]]

        case "macd":
            df["value"] = ta.macd(df["close"])["MACD_12_26_9"]
            df = df[["value"]]

        case "vwap":
            df["value"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
            df = df[["value"]]

        case "atr":
            df["value"] = ta.atr(df["high"], df["low"], df["close"], length=period)
            df = df[["value"]]

        case "obv":
            df["value"] = ta.obv(df["close"], df["volume"])
            df = df[["value"]]

        case "roc":
            df["value"] = ta.roc(df["close"], length=period)
            df = df[["value"]]

    df = df.dropna().tail(1)
    if len(df) > 0:
        df = df.dropna().tail(1)
        return df
    return None
    # need to make these so that it all return a slide with value is a value also with time


def get_all_data_point(
    df: pd.DataFrame, field: str | None, indicator: str | None, period: int | None
):
    match indicator:
        case None:
            df = df[field].dropna().to_frame()
        case "ma":
            df["MA"] = ta.sma(df[field], length=period)
            # Create a new DataFrame with only the time and the indicator
            df = df[["MA"]].dropna()

        case "ema":
            df["EMA"] = ta.ema(df[field], length=period)
            # Create a new DataFrame with only the time and the indicator
            df = df[["EMA"]].dropna()
            # Rename columns if necessary
            # df.rename(columns={'index': '_time', 'EMA': 'Indicator'}, inplace=True)
        case "stoch":
            df[["STOCH_k", "STOCH_d"]] = ta.stoch(df["high"], df["low"], df["close"])
            # Create a new DataFrame with only the time and the indicator
            df = df[["STOCH_k", "STOCH_d"]].dropna()

        case "rsi":
            df["RSI"] = ta.rsi(df["close"], length=period)
            df = df[["RSI"]].dropna()

        case "macd":
            df[["MACD", "MACD_h", "MACD_s"]] = ta.macd(df["close"])
            df = df[["MACD", "MACD_h", "MACD_s"]].dropna()

        case "vwap":
            df["VWAP"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
            df = df[["VWAP"]]

        case "atr":
            df["ATR"] = ta.atr(df["high"], df["low"], df["close"], length=period)
            df = df[["ATR"]]

        case "obv":
            df["OBV"] = ta.obv(df["close"], df["volume"])
            df = df[["OBV"]]

        case "roc":
            df["ROC"] = ta.roc(df["close"], length=period)
            df = df[["ROC"]]
    return df

    
'''
def get_latest_data_point(df: pd.DataFrame, field: str | None, indicator: str | None, period: int | None):
    if indicator is None:
        df = df[[field]]
        df.columns = ["value"]
    elif indicator == "ma":
        df["value"] = ta.sma(df[field], length=period)
        df = df[["value"]]
    elif indicator == "ema":
        df["value"] = ta.ema(df[field], length=period)
        df = df[["value"]]
    elif indicator == "stoch_k":
        df["STOCH_k"] = ta.stoch(df["high"], df["low"], df["close"]).iloc[:, 0]
        df = df[["STOCH_k"]].rename(columns={"STOCH_k": "value"})
    elif indicator == "stoch_d":
        df["STOCH_d"] = ta.stoch(df["high"], df["low"], df["close"]).iloc[:, 1]
        df = df[["STOCH_d"]].rename(columns={"STOCH_d": "value"})
    elif indicator == "rsi":
        df["value"] = ta.rsi(df["close"], length=period)
        df = df[["value"]]
    elif indicator == "macd":
        df["value"] = ta.macd(df["close"])["MACD_12_26_9"]
        df = df[["value"]]
    elif indicator == "vwap":
        df["value"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        df = df[["value"]]
    elif indicator == "atr":
        df["value"] = ta.atr(df["high"], df["low"], df["close"], length=period)
        df = df[["value"]]
    elif indicator == "obv":
        df["value"] = ta.obv(df["close"], df["volume"])
        df = df[["value"]]
    elif indicator == "roc":
        df["value"] = ta.roc(df["close"], length=period)
        df = df[["value"]]
    else:
        raise ValueError(f"Unknown indicator: {indicator}")

    df = df.dropna().tail(1)
    if len(df) > 0:
        return df
    return None

def get_all_data_point(df: pd.DataFrame, field: str | None, indicator: str | None, period: int | None):
    if indicator is None:
        df = df[field].dropna().to_frame()
    elif indicator == "ma":
        df["MA"] = ta.sma(df[field], length=period)
        df = df[["MA"]].dropna()
    elif indicator == "ema":
        df["EMA"] = ta.ema(df[field], length=period)
        df = df[["EMA"]].dropna()
    elif indicator == "stoch":
        df[["STOCH_k", "STOCH_d"]] = ta.stoch(df["high"], df["low"], df["close"])
        df = df[["STOCH_k", "STOCH_d"]].dropna()
    elif indicator == "rsi":
        df["RSI"] = ta.rsi(df["close"], length=period)
        df = df[["RSI"]].dropna()
    elif indicator == "macd":
        df[["MACD", "MACD_h", "MACD_s"]] = ta.macd(df["close"])
        df = df[["MACD", "MACD_h", "MACD_s"]].dropna()
    elif indicator == "vwap":
        df["VWAP"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        df = df[["VWAP"]]
    elif indicator == "atr":
        df["ATR"] = ta.atr(df["high"], df["low"], df["close"], length=period)
        df = df[["ATR"]]
    elif indicator == "obv":
        df["OBV"] = ta.obv(df["close"], df["volume"])
        df = df[["OBV"]]
    elif indicator == "roc":
        df["ROC"] = ta.roc(df["close"], length=period)
        df = df[["ROC"]]
    else:
        raise ValueError(f"Unknown indicator: {indicator}")

    return df

'''


def caculate_analaytic_data(df: pd.DataFrame):
    print("done///")

    print(ta.sma(df["close"]))
    print(ta.rsi(df["close"]))
    
    print("////////////////////////////////////////////")
    df = df[["close", "high", "low", "volume"]]
   
    
    

    df = (
        pd.concat(
            [
                df,
                ta.sma(df["close"]),
                ta.macd(df["close"]).iloc[:, :1],
                ta.rsi(df["close"]),          
                ta.stoch(df["high"], df["low"], df["close"]).iloc[:, :1],       
                ta.obv(df["close"], df["volume"]),
                ta.bbands(df["close"]).iloc[:, :3],
                ta.atr(df["high"], df["low"], df["close"]),   

            ],
            axis=1,
        )
        .dropna()
        .round(2)
    )
    

    # df.columns = [re.sub(r"[_\d.]", "", name) for name in df.columns]
    # df.index = df.index.tz_convert("Asia/Ho_Chi_minh").strftime("%Y-%m-%d")
    # df.reset_index(inplace=True)
    # # df.to_csv("./test.csv")
    # df.to_json(orient="records")
    print(df)
    return df


