from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)

# ==========================================================
# Load Superstore Dataset
# ==========================================================

try:
    df = pd.read_csv(
        "Sample - Superstore.csv",
        encoding="latin1"
    )

    print("=" * 60)
    print("Sample Superstore Dataset Loaded Successfully")
    print(f"Total Records : {len(df)}")
    print(f"Total Columns : {len(df.columns)}")
    print("=" * 60)

except Exception as e:
    print("Error Loading Dataset")
    print(e)
    df = pd.DataFrame()

# ==========================================================
# Home Route
# ==========================================================

@app.route("/")
def home():

    return jsonify({
        "message": "Retail Sales API is Running",
        "dataset": "Sample Superstore",
        "total_records": len(df)
    })


# ==========================================================
# Complete Dataset
# ==========================================================

@app.route("/api/sales", methods=["GET", "POST"])
def get_sales():

    global df

    if request.method == "POST":
        try:
            new_record = request.get_json()
            if not new_record:
                return jsonify({"error": "No data provided"}), 400
            
            # Simple validation to check if row_id is provided
            if "Row ID" not in new_record:
                return jsonify({"error": "Row ID is required"}), 400
                
            # Append new record to df
            new_df = pd.DataFrame([new_record])
            df = pd.concat([df, new_df], ignore_index=True)
            
            return jsonify({"message": "Record added successfully", "total_records": len(df)}), 201

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify(
        df.to_dict(orient="records")
    )


# ==========================================================
# First 100 Records
# ==========================================================

@app.route("/api/sales/sample", methods=["GET"])
def sample_sales():

    sample = df.head(100)

    return jsonify(
        sample.to_dict(orient="records")
    )


# ==========================================================
# Dataset Summary
# ==========================================================

@app.route("/api/summary", methods=["GET"])
def summary():

    return jsonify({

        "Rows": len(df),
        "Columns": len(df.columns),
        "Column Names": list(df.columns)

    })


# ==========================================================
# Column Names
# ==========================================================

@app.route("/api/columns", methods=["GET"])
def columns():

    return jsonify(

        list(df.columns)

    )


# ==========================================================
# Check API Status
# ==========================================================

@app.route("/api/status", methods=["GET"])
def status():

    return jsonify({

        "status": "SUCCESS",
        "api": "Running",
        "records_available": len(df)

    })


# ==========================================================
# Customer Count
# ==========================================================

@app.route("/api/customers", methods=["GET"])
def customers():

    count = df["Customer ID"].nunique()

    return jsonify({

        "Unique Customers": int(count)

    })


# ==========================================================
# Product Count
# ==========================================================

@app.route("/api/products", methods=["GET"])
def products():

    count = df["Product ID"].nunique()

    return jsonify({

        "Unique Products": int(count)

    })


# ==========================================================
# Total Sales
# ==========================================================

@app.route("/api/totalsales", methods=["GET"])
def totalsales():

    total = df["Sales"].sum()

    return jsonify({

        "Total Sales": float(total)

    })


# ==========================================================
# Total Profit
# ==========================================================

@app.route("/api/totalprofit", methods=["GET"])
def totalprofit():

    total = df["Profit"].sum()

    return jsonify({

        "Total Profit": float(total)

    })


# ==========================================================
# Start Flask
# ==========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )