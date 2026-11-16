import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Evaluate 50 Hand-Labeled Messages")
    parser.add_argument("--csv_path", default="outputs/part2/manual_50_sample.csv", help="Completed 50-sample CSV")
    args, _ = parser.parse_known_args()

    df = pd.read_csv(args.csv_path)
    df["human_label"] = df["human_label"].fillna("").str.strip()
    df["predicted_class"] = df["predicted_class"].str.strip()

    total = len(df)
    none_df = df[df["human_label"].str.lower() == "none of these"]
    in_domain = df[df["human_label"].str.lower() != "none of these"]

    correct = (in_domain["human_label"] == in_domain["predicted_class"]).sum()
    acc = correct / len(in_domain) if len(in_domain) > 0 else 0.0

    print(f"Total messages reviewed:        {total}")
    print(f"'None of these' count:          {len(none_df)} ({len(none_df)/total:.1%})")
    print(f"In-domain messages (4 classes): {len(in_domain)}")
    print(f"In-domain accuracy:             {correct}/{len(in_domain)} = {acc:.2%}")

    print("\n--- Misclassified In-Domain Examples ---")
    wrong = in_domain[in_domain["human_label"] != in_domain["predicted_class"]]
    for _, row in wrong.head(5).iterrows():
        print(
            f"- Human: {row['human_label']} | Model: {row['predicted_class']} "
            f"(p={row['top_probability']:.2f}) | Text: {row['text'][:120]}"
        )


if __name__ == "__main__":
    main()
