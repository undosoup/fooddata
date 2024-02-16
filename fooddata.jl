import ZipFile
using CSV
using DataFrames
using Downloads
using Memoization
import StringEncodings

# fetch data from https://www.fao.org/faostat/en/#data/QCL
# FAOSTAT -> Data -> Production -> Crops and Livestock Products
#
# Data is a ZIP archive containing:
#
# - "Production_Crops_Livestock_E_All_Data.csv"
# - "Production_Crops_Livestock_E_All_Data_NOFLAG.csv"
# - "Production_Crops_Livestock_E_AreaCodes.csv"
# - "Production_Crops_Livestock_E_Flags.csv"
# - "Production_Crops_Livestock_E_ItemCodes.csv"
#
data_url = "https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Data.zip"
data_path = "Production_Crops_Livestock_E_All_Data.zip"
if !isfile(data_path)
    Base.download(data_url), data_path
end
data_zip = ZipFile.Reader(data_path)

load_table = @memoize function(name)
    filename = "Production_Crops_Livestock_E_$(name).csv"
    f = first(filter(f -> f.name == filename, data_zip.files))
    seek(f, 0) |> CSV.File |> DataFrame
end
all_data() = load_table("All_Data")
all_data_no_flag() = load_table("All_Data_NOFLAG")
area_codes() = load_table("AreaCodes")
flags() = load_table("Flags")
item_codes() = load_table("ItemCodes")

year(y, df) = df[:, "Y$y"]
year(y) = year_data(y, all_data())

lentils(df) = filter(:Item => startswith("Lentils"), df)
lentils() = lentils(all_data())

production(df) = filter(:Element => ==("Production"), df)
production() = production(all_data())

# This excludes regional totals
# Most of these are continent-ish regions with codes >=5000
# "China" (351) includes mainland China, Taiwan, Hong Kong, and Macau
country(df) = filter(Symbol("Area Code") => <(300), df)
country() = country(all_data())

# The area names are not UTF-8 encoded
# This affects 3 areas:
# julia> filter(:Area => (x -> !isvalid(x)), area_codes())
#
#   1 │       107  '384      C\xf4te d'Ivoire
#   2 │       182  '638      R\xe9union
#   3 │       223  '792      T\xfcrkiye
#
# This is probably Latin-1 or Windows-1252
fix_encoding(s) = StringEncodings.decode(codeunits(s), "windows-1252")

# this is coalesce(x, 0.0)
# or coalesce.(df, 0.0) for a dataframe
# missing_to_zero(x) = ismissing(x) ? 0.0 : x
