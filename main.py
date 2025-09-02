import requests
import polars as pl
from io import StringIO

def try_api(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    except:
        print(response.status_code)
        return None

def main():
    api = "https://download.bls.gov/pub/time.series"
    endpoints = {
        "ProducerPriceConstructionData": "pc/pc.data.75.Construction",
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

    categories = endpoints.keys()
    for cat in categories:

        url = f"{api}/{endpoints[cat]}"
        
        response = try_api(url, headers)
        if response is None:
            continue

        text="\n".join([" ".join(line.split()) for line in response.text.splitlines()])
        df = pl.read_csv(StringIO(text), separator=" ")

        df1 = df.filter(1==1) \
            .filter(
                (pl.col('period').str.starts_with('S')==False)
                & (pl.col('period') != 'M13')
            ) \
            .with_columns(
                pl.col('period').str.slice(-2).cast(pl.Int8)
            ) \
            .with_columns(
                pl.date(pl.col('year'), pl.col('period'), 1).alias('date'),
            ) \
            .with_columns(
                pl.col('date').dt.strftime("%B").alias('MonthName'),
                pl.col('period')#.cast(pl.String)
            ) \
            .filter(
                (1==1)
                & (pl.col('date') >= pl.date(1975, 1, 1)) 
                & (pl.col('series_id').is_in(['CUUR0000SAA','CUSR0000SAA']))
            ) \
            .group_by(['series_id', 'year', 'period', 'date', 'MonthName']) \
            .agg(pl.mean('value')) \
            .sort(['series_id', 'year', 'period']) \
            .pivot(on='series_id', index=['year', 'period', 'date', 'MonthName'], values='value') \
            .select(['year', 'period', 'date', 'MonthName', 'CUUR0000SAA', 'CUSR0000SAA'])
        
        df2 = df.filter(1==1) \
            .filter(
                (pl.col('period').str.starts_with('S')==False)
                & (pl.col('period') == 'M13')
            ) \
            .with_columns(
                pl.col('period').str.slice(-2).cast(pl.Int8)
            ) \
            .filter(
                (pl.col('year') >= 1975)
                & (pl.col('series_id').is_in(['CUUR0000SAA','CUSR0000SAA']))
            ) \
            .with_columns(
                (pl.col('value')*0).alias('CUSR0000SAA'),
                pl.col('value').alias('CUUR0000SAA'),
                # pl.col('period').replace_strict(13, 'Annual'),
                pl.lit(None).alias('date'),
                pl.lit(None).alias('MonthName'),
            ) \
            .select(['year', 'period', 'date', 'MonthName', 'CUUR0000SAA', 'CUSR0000SAA'])
        
        df3 = pl.concat([df1, df2])
        df3.columns = ['Year', 'MonthNum', 'M/YYYY', 'Month', 'Not Seasonally Adjusted', 'Seasonally Adjusted']
        df4 = df3.filter(1==1) \
            .sort(['Year', 'MonthNum']) \
            .with_columns(
                pl.col('MonthNum').alias('Period'),
                pl.col('M/YYYY').dt.strftime('%m-%Y'),
                pl.col('MonthNum').cast(pl.String).replace({'13': 'Annual'}),
            )
        
        df4.head()


if __name__ == "__main__":
    main()
