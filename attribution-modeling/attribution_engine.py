# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 16:06:50 2026

@author: abdul
"""

from __future__ import annotations
import numpy as np
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_COLUMNS', None)
# Define a static global list of valid marketing distribution channels
CHANNELS = ["Paid Search", "Paid Social", "Organic", "Partner", "Event", "Email"]

# Define a static global list of valid core b2b marketing campaign types
CAMPAIGN_TYPES = ["Demand Gen", "Product Webinar", "Content Syndication", "Partner Campaign", "Retargeting"]


# Build a comprehensive pair of mock datasets modeling real CRM and marketing logs
def build_sample_data(seed: int = 19) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Initialize NumPy's recommended random generator engine with a fixed seed for consistency
    rng = np.random.default_rng(seed)
    
    # Grab the current calendar date stamp and normalize it to remove time elements (midnight)
    today = pd.Timestamp.today().normalize()

    # Create an empty list container to accumulate dictionary records for opportunities
    opportunities = []
    
    # Run a loop to assemble exactly 150 unique simulated sales pipeline deals
    for i in range(1, 151):
        # Calculate a random historic creation date falling anywhere from 45 to 220 days ago
        created_at = today - pd.Timedelta(days=int(rng.integers(45, 220)))
        
        # Calculate a realistic close date by adding 25 to 140 operational days to creation
        close_date = created_at + pd.Timedelta(days=int(rng.integers(25, 140)))
        
        # Randomly assign a sales deal outcome based on realistic percentage weight splits
        stage = rng.choice(["Closed Won", "Closed Lost", "Open"], p=[0.42, 0.24, 0.34])

        # If a transaction is actively 'Open', override and wipe out the close date property
        if stage == "Open":
            close_date = pd.NaT # Use Pandas Not-a-Time marker to represent null elements

        # Push the freshly generated individual sales record into the staging array
        opportunities.append(
            {
                "opportunity_id": f"opp_{i:05d}",                       # Pad IDs like 'opp_00001'
                "account_id": f"acct_{rng.integers(1, 90):04d}",         # Link to 1 of 90 account IDs
                "stage_name": stage,                                     # Assign the active status
                "created_at": created_at,                               # Assign creation day
                "close_date": close_date,                               # Assign termination day
                "amount": float(rng.integers(10_000, 180_000)),         # Assign random monetary deal valuation
            }
        )

    # Cast the populated list structure into an indexable Pandas DataFrame object
    opportunities_df = pd.DataFrame(opportunities)

    # Create an empty list container to compile individual marketing touch interaction records
    touchpoints = []
    
    # Initialize a basic operational counter tracking sequential index values for interactions
    touch_id = 1

    # Loop through each row of the opportunities table as named tuples for maximum performance
    for opportunity in opportunities_df.itertuples(index=False):
        # Generate a random interaction volume count between 1 and 7 marketing touchpoints per deal
        touch_count = int(rng.integers(1, 8))
        
        # Establish a valid cut-off date ceiling using deal closure or defaulting to today if open
        end_date = opportunity.close_date if pd.notna(opportunity.close_date) else today

        # Iterate through the generated loop quantity to build individual touchpoints
        for _ in range(touch_count):
            # Model early awareness activity by letting touches occur up to 45 days BEFORE deal creation
            touch_date = opportunity.created_at - pd.Timedelta(days=int(rng.integers(0, 45)))
            
            # Add forward days to distribute touches across the active sales cycle window
            touch_date = touch_date + pd.Timedelta(days=int(rng.integers(0, max((end_date - opportunity.created_at).days, 1))))
            
            # Select a random marketing execution channel out of our predefined options
            channel = rng.choice(CHANNELS)
            
            # Select a random operational campaign strategy type out of our options
            campaign_type = rng.choice(CAMPAIGN_TYPES)

            # Package and append the marketing interaction data snapshot directly to our collection list
            touchpoints.append(
                {
                    "touchpoint_id": f"touch_{touch_id:06d}",            # Pad IDs like 'touch_000001'
                    "opportunity_id": opportunity.opportunity_id,        # Link transaction relationship
                    "account_id": opportunity.account_id,                # Link target corporate account relationship
                    "campaign_id": f"camp_{rng.integers(1, 35):03d}",    # Map back to 1 of 35 distinct marketing efforts
                    "campaign_type": campaign_type,                      # Store classification
                    "channel": channel,                                  # Store routing method
                    "touch_date": touch_date,                            # Store specific activity day
                }
            )
            # Increment the tracking identifier integer upwards to keep values globally unique
            touch_id += 1

    # Cast the complete aggregated interaction ledger into a standard Pandas DataFrame layout
    touchpoints_df = pd.DataFrame(touchpoints)

    # Injecting intentional anomalies to test data-validation guardrails:
    
    # Duplicate Data Anomaly: Clone the 4th row index entry exactly
    duplicate = touchpoints_df.iloc[[3]].copy()
    
    # Modify the identifier value on the duplicate object to create a business-key duplication error
    duplicate["touchpoint_id"] = "touch_duplicate_example"
    
    # Concatenate the broken row back into the primary collection array, resetting indexes cleanly
    touchpoints_df = pd.concat([touchpoints_df, duplicate], ignore_index=True)

    # Missing Data Anomaly: Intentionally drop the strategy type context on row index 8 to None
    touchpoints_df.loc[8, "campaign_type"] = None
    
    # Orphan Record Anomaly: Break referential integrity on row index 17 by assigning a non-existent deal ID
    touchpoints_df.loc[17, "opportunity_id"] = "opp_missing"

    # Out of Bounds Time Anomaly: Find the first closed opportunity to test timing rules
    closed_opps = opportunities_df[opportunities_df["stage_name"].isin(["Closed Won", "Closed Lost"])].head(1)
    
    # Confirm a closed opportunity was successfully found before attempting to inject timing bugs
    if not closed_opps.empty:
        # Create a deep copy of row index 20 to use as our testing template
        after_close = touchpoints_df.iloc[[20]].copy()
        
        # Rewrite the record metadata properties to explicitly anchor to a clean example label
        after_close["touchpoint_id"] = "touch_after_close_example"
        
        # Link this touchpoint explicitly back to the known closed deal we isolated
        after_close["opportunity_id"] = closed_opps.iloc[0]["opportunity_id"]
        
        # Link the account mapping properties cleanly to match the target closed deal profile
        after_close["account_id"] = closed_opps.iloc[0]["account_id"]
        
        # Push the interaction date 4 full days PAST the official close date to trigger timing errors
        after_close["touch_date"] = closed_opps.iloc[0]["close_date"] + pd.Timedelta(days=4)
        
        # Append this out-of-bounds record back into the core database dataframe collection
        touchpoints_df = pd.concat([touchpoints_df, after_close], ignore_index=True)

    # Return both dataframes back to the main coordination layer as a clean tuple payload
    return opportunities_df, touchpoints_df


# Sanitize raw tracking data formats and fill missing values with clean defaults
def clean_inputs(opportunities: pd.DataFrame, touchpoints: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Clone the incoming opportunity table to avoid modifying original source data
    opps = opportunities.copy()
    
    # Clone the incoming touchpoint table to preserve source data integrity
    touches = touchpoints.copy()

    # Loop through columns that need to be explicitly cast as datetime objects
    for column in ["created_at", "close_date"]:
        # Coerce invalid parsing formats or null indicators directly into NaT objects safely
        opps[column] = pd.to_datetime(opps[column], errors="coerce")

    # Format the touchpoint date column into clean datetime representations, coercing errors
    touches["touch_date"] = pd.to_datetime(touches["touch_date"], errors="coerce")
    
    # Impute missing campaign type classifications with an explicit 'Unknown' string flag
    touches["campaign_type"] = touches["campaign_type"].fillna("Unknown")
    
    # Impute missing channel classifications with an explicit 'Unknown' string flag
    touches["channel"] = touches["channel"].fillna("Unknown")

    # Hand back the formatted, structurally safe datasets
    return opps, touches


# Analyze operational business logic loops to catch and log specific dataset quality anomalies
def flag_touchpoint_quality(opportunities: pd.DataFrame, touchpoints: pd.DataFrame) -> pd.DataFrame:
    # Isolate relevant opportunity columns to form a reference lookup table
    opp_lookup = opportunities[["opportunity_id", "account_id", "created_at", "close_date", "stage_name"]].copy()
    
    # Left join touchpoint logs against our lookup table using the opportunity ID column
    joined = touchpoints.merge(opp_lookup, on="opportunity_id", how="left", suffixes=("", "_opp"))

    # Initialize a blank string column to store data quality issues
    joined["issue_type"] = ""
    
    # Anomaly Rule 1: If the lookup field account ID is completely missing, flag an unlinked orphan record
    joined.loc[joined["account_id_opp"].isna(), "issue_type"] = "opportunity_not_found"
    
    # Anomaly Rule 2: If the touchpoint account ID doesn't match the deal's account ID, flag a mismatch
    joined.loc[joined["account_id_opp"].notna() & joined["account_id"].ne(joined["account_id_opp"]), "issue_type"] = "account_mismatch"
    
    # Anomaly Rule 3: Catch records containing our placeholder 'Unknown' string tags
    joined.loc[joined["campaign_type"].eq("Unknown"), "issue_type"] = "missing_campaign_type"
    
    # Anomaly Rule 4: Catch records where the timestamp conversion failed and left a missing date
    joined.loc[joined["touch_date"].isna(), "issue_type"] = "missing_touch_date"

    # Anomaly Rule 5: Check if the interaction took place after the deal was already closed
    after_close = joined["close_date"].notna() & joined["touch_date"].gt(joined["close_date"])
    
    # Apply the out-of-bounds flag to rows that violate the timeline rule
    joined.loc[after_close, "issue_type"] = "touch_after_close"

    # Anomaly Rule 6: Group fields together to construct a composite business key to evaluate duplicates
    duplicate_business_key = ["opportunity_id", "account_id", "campaign_id", "channel", "touch_date"]
    
    # Mark every occurrence of identical data rows matching that composite blueprint pattern
    duplicate_mask = joined.duplicated(subset=duplicate_business_key, keep=False)
    
    # Apply a duplicate flag only to records that haven't already been caught by a previous rule
    joined.loc[duplicate_mask & joined["issue_type"].eq(""), "issue_type"] = "duplicate_touchpoint"

    # Filter out everything that passed perfectly, leaving only the rows that triggered an issue flag
    quality_flags = joined[joined["issue_type"].ne("")].copy()
    
    # Return a structured subset of columns containing the detailed audit trail info
    return quality_flags[
        [
            "touchpoint_id",
            "opportunity_id",
            "account_id",
            "campaign_id",
            "campaign_type",
            "channel",
            "touch_date",
            "issue_type",
        ]
    ]


# Filter out flawed data records and build a clean, chronologically sequenced fact table
def build_clean_touchpoint_fact(opportunities: pd.DataFrame, touchpoints: pd.DataFrame) -> pd.DataFrame:
    # Select the minimal set of columns from the opportunities table needed to allocate revenue split values
    opp_fields = opportunities[["opportunity_id", "account_id", "created_at", "close_date", "stage_name", "amount"]]
    
    # Use an inner join to discard unlinked orphan records or mismatched account profiles automatically
    joined = touchpoints.merge(opp_fields, on=["opportunity_id", "account_id"], how="inner")

    # Time-Boundary Guard: Keep interactions only if the deal is open or if the touch occurred before closure
    before_close_or_open = joined["close_date"].isna() | joined["touch_date"].le(joined["close_date"])
    
    # Apply the time-boundary filter to the active dataframe
    joined = joined[before_close_or_open].copy()

    # Deduplication Guard: Define the composite business key layout to target redundancy
    business_key = ["opportunity_id", "account_id", "campaign_id", "channel", "touch_date"]
    
    # Sort everything chronologically by deal and timestamp to keep records organized
    joined = joined.sort_values(["opportunity_id", "touch_date", "touchpoint_id"])
    
    # Drop redundant duplicates from the stream, keeping only the first record that occurred
    joined = joined.drop_duplicates(subset=business_key, keep="first")

    # Sequencing: Calculate a sequential timeline counter (1, 2, 3...) for touchpoints inside each deal group
    joined["touch_sequence"] = joined.groupby("opportunity_id").cumcount() + 1
    
    # Aggregate and broadcast the absolute total touchpoint volume count back onto every related deal row
    joined["touch_count"] = joined.groupby("opportunity_id")["touchpoint_id"].transform("count")
    
    # Flag a boolean True marker if the specific sequence number equals 1 (First Touch)
    joined["is_first_touch"] = joined["touch_sequence"].eq(1)
    
    # Flag a boolean True marker if the sequence count matches the total touch volume (Last Touch)
    joined["is_last_touch"] = joined["touch_sequence"].eq(joined["touch_count"])

    # Calculate operational delays: Use the close date if available, otherwise default to today's date
    close_or_today = joined["close_date"].fillna(pd.Timestamp.today().normalize())
    
    # Calculate the elapsed age in days between the baseline date and the marketing interaction
    joined["days_before_close_or_today"] = (close_or_today - joined["touch_date"]).dt.days

    # Return the clean, organized fact dataset
    return joined


# Apply multiple revenue attribution models to divide deal values among marketing touchpoints
def assign_influence(clean_touchpoints: pd.DataFrame) -> pd.DataFrame:
    # Clone the clean fact table to avoid mutating state outside the function scope
    df = clean_touchpoints.copy()

    # First-Touch Model: Assign 100% of the financial value if it's the first interaction, else 0
    df["first_touch_influence"] = np.where(df["is_first_touch"], df["amount"], 0)
    
    # Last-Touch Model: Assign 100% of the financial value if it's the final interaction, else 0
    df["last_touch_influence"] = np.where(df["is_last_touch"], df["amount"], 0)
    
    # Even Split Model: Divide the deal value equally by the total number of interactions found on that deal
    df["even_touch_influence"] = df["amount"] / df["touch_count"].replace(0, np.nan)

    # Position-Weighted Model (U-Shaped): Assign custom weight splits based on touchpoint sequence placement
    df["position_weight"] = np.where(
        df["touch_count"].eq(1),
        1.0, # If there is only 1 touchpoint, give it 100% of the weight
        np.where(
            df["is_first_touch"] | df["is_last_touch"], 
            0.4, # Give 40% weight to the first touch and 40% to the last touch
            0.2 / (df["touch_count"] - 2).clip(lower=1) # Distribute the remaining 20% evenly across middle touches
        ),
    )

    # Sum up the weights within each opportunity group to ensure everything adds up to 100%
    weight_total = df.groupby("opportunity_id")["position_weight"].transform("sum")
    
    # Normalize the weights by dividing each weight by its group total to fix rounding anomalies
    df["position_weight"] = df["position_weight"] / weight_total
    
    # Multiply the deal amount by the normalized position weight to get the final dollar credit
    df["weighted_influence"] = df["amount"] * df["position_weight"]

    # Return the enriched attribution dataframe
    return df


# Aggregate performance data to show pipeline credit by campaign type and channel
def summarize_campaign_influence(influenced_touchpoints: pd.DataFrame) -> pd.DataFrame:
    # Group the dataset by the campaign strategy and execution channel
    summary = (
        influenced_touchpoints.groupby(["campaign_type", "channel"], dropna=False)
        .agg(
            touchpoints=("touchpoint_id", "count"),                 # Count total interaction rows
            opportunities=("opportunity_id", "nunique"),            # Count distinct sales deals influenced
            first_touch_pipeline=("first_touch_influence", "sum"),  # Total revenue from First-Touch
            last_touch_pipeline=("last_touch_influence", "sum"),    # Total revenue from Last-Touch
            even_touch_pipeline=("even_touch_influence", "sum"),    # Total revenue from Even Split
            weighted_pipeline=("weighted_influence", "sum"),        # Total revenue from Position-Weighted
        )
        .reset_index() # Flatten the index grouping into normal dataframe columns
        .sort_values("weighted_pipeline", ascending=False) # Sort results by highest weighted pipeline performance
    )

    # Return the clean summary report table
    return summary


# Render the explicit data-health summary report directly inline to the console
def print_method_note(quality_flags: pd.DataFrame) -> None:
    # Count the frequencies of each recorded data anomaly flag type
    issue_summary = quality_flags["issue_type"].value_counts().sort_index()
    
    # Format each data quality issue type and its count into a clean text line item
    issue_lines = [f" - {issue}: {count}" for issue, count in issue_summary.items()]

    # If the checklist evaluated cleanly without entries, append a positive status line instead
    if not issue_lines:
        issue_lines.append(" - No touchpoint quality issues found.")

    # Construct the documentation report structure body using an explicit Python multi-line f-string
    note = f"""
TOUCHPOINT INFLUENCE METHOD NOTE
This is a campaign influence model dashboard using inline processed synthetic CRM data.

The system execution tracking pipeline completed:
 1. Joined marketing touch interactions directly to pipeline opportunities
 2. Screened and isolated row-level quality anomalies 
 3. Dropped corrupted, orphan, or duplicate events from downstream calculations
 4. Computed revenue distributions: First-Touch, Last-Touch, Even-Split, and Position-Weighted

Quality anomalies captured during this analysis sequence:
{chr(10).join(issue_lines)}

Model Caveat & Warning:
Attribution modeling provides logical perspectives based on clean input patterns; it is not absolute.
This system acts to surface assumptions clearly and identify dirty data workflows before scoring.
"""
    # Stream the formatted report string directly to the terminal stdout view
    print(note)


# Render a clean, text-based inline horizontal bar chart visualization to the console screen
def print_inline_influence_chart(summary: pd.DataFrame) -> None:
    # Isolate the top 8 highest-performing campaign combinations from the summary table dataset
    chart_data = summary.head(8).copy()
    
    # Extract the absolute maximum financial pipeline performance to calibrate our character width limits
    max_val = chart_data["weighted_pipeline"].max() if not chart_data.empty else 1

    # Print a top section separation border to isolate the inline graphic
    print("\n" + "=" * 100)
    print("VISUALIZATION CHART: TOP CAMPAIGN PERFORMANCE (BY WEIGHTED PIPELINE)")
    print("-" * 100)

    # Loop through our top records to draw customized horizontal textual bar fills
    for row in chart_data.itertuples(index=False):
        # Concatenate campaign components together to generate a clean category descriptor string
        label = f"{row.campaign_type} ({row.channel})"
        
        # Calculate how many hash mark characters to print by checking the row value against max limits
        bar_length = int((row.weighted_pipeline / max_val) * 40) if max_val > 0 else 0
        
        # Build the horizontal bar metric row out of repeating hash marks
        bar_visual = "#" * bar_length
        
        # Output the formatted structural category name, graphic fill, and actual formatted dollar value
        print(f"{label:<45} | {bar_visual:<40} | ${row.weighted_pipeline:,.2f}")

    # Print a matching bottom completion boundary row to close the visualization segment
    print("=" * 100 + "\n")



# Step 1: Build the raw mock datasets containing hidden data anomalies
opportunities, touchpoints = build_sample_data()

# Step 2: Format inputs and standardize raw string fields and datetime columns
opportunities, touchpoints = clean_inputs(opportunities, touchpoints)

# Step 3: Run the auditing rules engine to flag and document data quality exceptions
quality_flags = flag_touchpoint_quality(opportunities, touchpoints)

# Step 4: Construct a clean fact table containing only valid, well-sequenced touchpoints
clean_touchpoints = build_clean_touchpoint_fact(opportunities, touchpoints)

# Step 5: Apply the revenue attribution models to divide pipeline values among touchpoints
influenced_touchpoints = assign_influence(clean_touchpoints)

# Step 6: Aggregate the results to see which campaign types and channels drove the most pipeline
summary = summarize_campaign_influence(influenced_touchpoints)

# Step 7: Print high-level inspection slices of the input tables inline to screen
print("\n" + "=" * 100)
print("INLINE DATA EXPORTS: CRM RAW SOURCE REVIEWS")
print("=" * 100)

# Print the top 5 records of the loaded sample sales opportunity table
print("\n--- SAMPLE OPPORTUNITIES (HEAD 5) ---")
print(opportunities.head(5).to_string(index=False))

# Print the top 5 records of the captured raw marketing touchpoint interaction table
print("\n--- SAMPLE MARKETING TOUCHPOINTS (HEAD 5) ---")
print(touchpoints.head(5).to_string(index=False))

# Step 8: Print the isolated row-level quality validation log tracking details inline
print("\n" + "=" * 100)
print("DATA AUDIT LOG: FLAG QUALITY CONSTRAINTS")
print("=" * 100)

# Handle display views safely if no exceptions or schema flaws are found
if quality_flags.empty:
    print("✓ Success: Clear validation run. No dataset quality anomalies captured.")
else:
    # Display the complete detailed tabular report of flagged input errors
    print(quality_flags.to_string(index=False))

# Step 9: Print the processed attribution fact metrics ledger inline to screen
print("\n" + "=" * 100)
print("FACT LEDGER: ENRICHED TOUCHPOINTS WITH MULTI-MODEL ATTRIBUTION REVENUE VALUES")
print("=" * 100)

# Define a clean viewing subset array of calculated revenue properties to inspect
view_cols = ["touchpoint_id", "opportunity_id", "campaign_type", "channel", "first_touch_influence", "last_touch_influence", "weighted_influence"]

# Print the top 10 records of the calculated multi-model split ledger
print(influenced_touchpoints[view_cols].head(10).to_string(index=False))

# Step 10: Print the final calculated overall channel contribution summaries inline
print("\n" + "=" * 100)
print("CAMPAIGN EFFICIENCY REPORT: FINAL AGGREGATED SUMMARY PERFORMANCE MATRIX")
print("=" * 100)

# Print the entire finalized summary dashboard result matrix directly to console view
print(summary.to_string(index=False))

# Step 11: Print the explanatory framework documentation text block inline
print_method_note(quality_flags)

# Step 12: Print the text-graphic performance horizontal breakdown visualization inline
print_inline_influence_chart(summary)
