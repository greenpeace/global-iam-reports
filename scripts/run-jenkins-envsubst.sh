cd ../report
# run template replace
for TEMPLATENAME in *-template; do
	FILENAME="${TEMPLATENAME%-template}"
	envsubst < "$TEMPLATENAME" > "$FILENAME"
done
