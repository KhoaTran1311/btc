#!/bin/bash

filename=$(date +"%Y_%m_%d__%H_%M")

declare -A links
links[cryptocompare]="https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=2000"

for key in "${!links[@]}"; do
	echo "Crawling $key"
	mkdir -p /home/k/Developers/btc/$key
	value="${links[$key]}"
	curl -s $value >> "/home/k/Developers/btc/$key/$filename"
done
