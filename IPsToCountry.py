import pandas as pd

# File paths
ipv4_file = "IPLocate-Country-GeoIPCompat-Blocks-IPv4.csv"
ipv6_file = "IPLocate-Country-GeoIPCompat-Blocks-IPv6.csv"
locations_file = "IPLocate-Country-GeoIPCompat-Locations-en.csv"
output_file = "IP_to_Country_Lookup.csv"

# Load locations fully (should be small)
locations_df = pd.read_csv(locations_file, usecols=["geoname_id", "country_name", "country_iso_code"])

# Function to process block files in chunks
def process_blocks(file_path, locations_df, chunk_size=500000):
    chunks = []
    for chunk in pd.read_csv(file_path, usecols=["network", "geoname_id"], chunksize=chunk_size):
        merged = chunk.merge(locations_df, on="geoname_id", how="left")
        merged = merged.rename(columns={"network": "cidr"})
        merged = merged[["cidr", "country_name", "country_iso_code"]]
        chunks.append(merged)
    return pd.concat(chunks, ignore_index=True)

# Process IPv4 and IPv6 blocks
ipv4_final = process_blocks(ipv4_file, locations_df)
ipv6_final = process_blocks(ipv6_file, locations_df)

# Combine both and save
final_df = pd.concat([ipv4_final, ipv6_final], ignore_index=True)
final_df.to_csv(output_file, index=False)

print(f"Lookup file created: {output_file}")
