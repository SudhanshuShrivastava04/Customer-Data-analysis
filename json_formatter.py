import pandas as pd

if __name__ == "__main__":
    data_path = r"../Customer-Data-Analysis-Starbucks/"

    # portfolio
    portfolio_df = pd.read_csv(data_path + "portfolio.csv", encoding="utf-8")
    portfolio_df["channels"] = portfolio_df["channels"].apply(
        lambda x: x.replace(r"'", r'"'))
    portfolio_df.to_csv(data_path + "portfolio_processed.csv",
                        encoding="utf-8",
                        index=None)

    # transcript
    transcript_df = pd.read_csv(data_path + "transcript.csv", encoding="utf-8")
    transcript_df["value"] = transcript_df["value"].apply(
        lambda x: x.replace(r"'", r'"'))
    transcript_df.to_csv(data_path + "transcript_processed.csv",
                         encoding="utf-8",
                         index=None)
