# Orchestrating the extraction and processing of Bureau of Labor Statistics (BLS) data

| Parameter    | Value | Output File Name |
| -------- | ------- | ------- |
| Consumer_Apparel | cu/cu.data.2.Summaries     | ConsumerPriceApparelData.xls |
| Consumer_Housing    | cu/cu.data.12.USHousing    | ConsumerPriceHousingData.xls |
| Consumer_Medical    | cu/cu.data.15.USMedical    | ConsumerPriceMedicalData.xls |
| Consumer_Recreation    | cu/cu.data.16.USRecreation    | ConsumerPriceRecreationData.xls |
| Producer_Finish_Goods    | wp/wp.data.22.FD-ID    | ProducerPriceData.xls |

### Series ID by Parameter
For each parameter series, the first and second series ID's are non-seasonally-adjusted and seasonally-adjusted, respectively.
- **Consumer_Apparel**
    - *Apparel*
        - CUUR0000SAA
        - CUSR0000SAA
- **Consumer_Housing**
    - *Household Furnishings and Operations*
        - CUUR0000SAH3
        - CUSR0000SAH3
- **Consumer_Medical**
    - *Physicians' Services*
        - CUUR0000SEMC01
        - CUSR0000SEMC01
    - *Hospital Services*
        - CUUR0000SEMD01
        - CUSR0000SEMD01
    - *Medical Care Commodities*
        - CUUR0000SAM1
        - CUSR0000SAM1
- **Consumer_Recreation**
    - *Recreation*
        - CUUR0000SAR
        - CUSR0000SAR
- **Producer_Finish_Goods**
    - *Finished Goods Less Energy*
        - WPUFD49208
        - WPSFD49208
    - *Finished Goods Less Food and Energy*
        - WPUFD4131
        - WPSFD4131