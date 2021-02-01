"""
Groupby memiliki konsep untuk :
1. Split: melakukan indexing/multi-indexing dengan apa yang di specify as groupby menjadi kelompok
2. Apply: menerapkan fungsi pada masing-masing kelompok tersebut
3. Combine: mengumpulkan semua hasil fungsi dari tiap kelompok kembali menjadi dataframe
"""
import pandas as pd

# Load data
global_air_quality = pd.read_csv("E:/DQLab/Dataset/global_air_quality_4000rows.csv")

# Cek info data
print(global_air_quality.info())

# Lima data teratas
print("\nData teratas :\n", global_air_quality.head())

"""------------------GROUPBY---------------------"""
print("\n[1] GROUPBY")
# Melakukan count() tanpa groupby
print("Count tanpa groupy :\n", global_air_quality.count())

# Melakukan count() dengan groupby
gaq_groupby_count = global_air_quality.groupby('source_name').count()
print("\nCount dengan groupy :\n", gaq_groupby_count.head())

print("\n[2] GROUPBY DAN AGGREGATION")
# Melakukan groupby dan aggregation dengan fungsi statistik dasar
# Buat variabel pollutant
pollutant = global_air_quality[['country', 'city', 'pollutant', 'value']].pivot_table(index=['country', 'city'],
                                                                                      columns='pollutant').fillna(0)
print("Data pollutant teratas:\n", pollutant.head())

# groupby berdasarkan 'country' dan terapkan aggregasi mean lalu nilai NaN ganti dengan 0
pollutant_mean = pollutant.groupby('country').mean().fillna(0)
print("\nMean pollutant:\n", pollutant_mean.head())

# groupby berdasarkan 'country' dan terapkan aggregasi nunique untuk mencari jumlah unique value tiap kolom
pollutant_unique = pollutant.groupby('country').nunique()
print("\nData pollutant unique :\n", pollutant_unique.head())

# groupby berdasarkan 'country' dan terapkan aggregasi first(), bisa juga last()
pollutant_mean = pollutant.groupby('country').first()
print("\nItem pertama pollutant:\n", pollutant_mean.head())

"""------------------GROUPBY MULTIPLE AGGREGATION---------------------"""
print("\n[3] GROUPBY MULTIPLE AGGREGATION")
# Group berdasarkan country dan terapkan aggregasi: min, median, mean, max
multiagg = pollutant.groupby('country').agg(['mean', 'median', 'min', 'max'])
print("Multi Aggregation :\n", multiagg.head())

print("\n[4] GROUPBY COSTUM AGGREGATION")


# Buat function iqr
def iqr(series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    return q3 - q1


# Group berdasarkan country dan terapkan aggregation quantile dari function iqr
costum_agg = pollutant.groupby('country').agg(iqr)
print("Costum Aggregation :\n", costum_agg.head())

print("\n[5] GROUPBY Custom Aggregations by dict")
# Create custom aggregation using dict
custom_agg_dict = pollutant['value'][['pm10', 'pm25', 'so2']].groupby('country').agg({
    'pm10': 'median',
    'pm25': iqr,
    'so2': iqr
})
print("Costum Aggregation by dict :\n", custom_agg_dict.head())
