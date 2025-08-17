output_file="$SCRIPT_DIR/../results/postgres_small_vp_point.txt"
database_name="generated1" #change name

fractions=("n0" "n20" "n40" "n60" "n80" "n100")

echo "----- Without Vertical Partitioning -----" > "$output_file"
for frac in "${fractions[@]}"; do
    query_file="queries/without_vp/${frac}.txt"
    if [[ -f "$query_file" ]]; then
        echo "--- Access Fraction $frac ---" >> "$output_file"
        bash ../base_postgres.sh "$database_name" "$(cat "$query_file")" >> "$output_file"
    else
        echo "Warning: Missing query file $query_file" >&2
    fi
done

echo "" >> "$output_file"

echo "----- With Vertical Partitioning -----" >> "$output_file"
for frac in "${fractions[@]}"; do
    query_file="queries/with_vp/${frac}.txt"
    if [[ -f "$query_file" ]]; then
        echo "--- Access Fraction $frac ---" >> "$output_file"
        bash ../base_postgres.sh "$database_name" "$(cat "$query_file")" >> "$output_file"
    else
        echo "Warning: Missing query file $query_file" >&2
    fi
done
