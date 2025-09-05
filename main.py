import requests
import polars as pl
from io import StringIO

api = "https://download.bls.gov/pub/time.series"
endpoints = {
    "ConsumerPriceApparelData": "cu/cu.data.2.Summaries",
    "ConsumerPriceHousingData": "cu/cu.data.12.USHousing",
    "ConsumerPriceMedicalData": "cu/cu.data.15.USMedical",
    "ConsumerPriceRecreationData": "cu/cu.data.16.USRecreation",
    "ProducerPriceData": "wp/wp.data.22.FD-ID",
}
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

series_ids = {
    "ConsumerPriceApparelData": {
        "CUUR0000SAA": "Apparel",
        "CUSR0000SAA": "Apparel",
    },
    "ConsumerPriceHousingData": {
        "CUUR0000SAH3": "Household Furnishings and Operations",
        "CUSR0000SAH3": "Household Furnishings and Operations",
    },
    "ConsumerPriceMedicalData": {
        "CUUR0000SEMC01": "Physicians' Services",
        "CUSR0000SEMC01": "Physicians' Services",
        "CUUR0000SEMD01": "Hospital Services",
        "CUSR0000SEMD01": "Hospital Services",
        "CUUR0000SAM1": "Medical Care Commodities",
        "CUSR0000SAM1": "Medical Care Commodities",
    },
    "ConsumerPriceRecreationData": {
        "CUUR0000SAR": "Recreation",
        "CUSR0000SAR": "Recreation",
    },
    "ProducerPriceData": {
        "WPUFD49208": "Finished Goods Less Energy",
        "WPSFD49208": "Finished Goods Less Energy",
        "WPUFD4131": "Finished Goods Less Food and Energy",
        "WPSFD4131": "Finished Goods Less Food and Energy",
    },
}

def generate_raw(df, cat: str):
    curr_srs = series_ids[cat]

    raw_df = df.filter(1==1) \
        .filter(
            (pl.col('period').str.starts_with('S')==False) # [S01, S02, S03]. not sure exactly what these are.
            & (pl.col('period') != 'M13') # Month 13 is the average of that series for that full calendar year.
            & (pl.col('series_id').is_in(list(curr_srs.keys())))
            & (pl.col('year') >= 1975) # Data is sparse before 1975
        ) \
        .with_columns(
            pl.col('period').str.slice(-2).cast(pl.Int8).alias('month'),
            pl.col('series_id').replace(curr_srs).alias("category"),
            (pl.col('series_id').str.slice(2, 1)=='S').alias('is_seasonally_adjusted'),
        ) \
        .with_columns(
            pl.date(pl.col('year'), pl.col('month'), 1).alias('date')
        ) \
        .select([
            'category',
            'series_id',
            'is_seasonally_adjusted',
            'date', 
            'value',
        ])
    
    return raw_df

def generate_pivot(raw_df):
    pivot_df = raw_df.filter(1==1) \
        .pivot(on=['series_id'], index=['date'], values=['value']) \
        .sort('date')
    
    return pivot_df

def generate_yr_agg_pivot(pivot_df):
    yr_agg_pivot_df = pivot_df.filter(1==1) \
        .with_columns(
            pl.col('date').dt.year().alias('year'),
        ) \
        .group_by(pl.col('year')) \
        .agg(pl.all().exclude('year', 'date').mean()) \
        .sort('year') \
        .select(pl.all().exclude("^..S.*$")) # remove seasonally adjusted pivot columns)

    return yr_agg_pivot_df

def main():
    categories = endpoints.keys()
    for cat in categories:
        url = f"{api}/{endpoints[cat]}"

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        text="\n".join([" ".join(line.split()) for line in response.text.splitlines()])
        df = pl.read_csv(StringIO(text), separator=" ")

        raw_df = generate_raw(df, cat)
        pivot_df = generate_pivot(raw_df)
        yr_agg_pivot_df = generate_yr_agg_pivot(pivot_df)

if __name__ == "__main__":
    main()
