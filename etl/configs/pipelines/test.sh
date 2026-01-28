#!/bin/bash
time for i in vlastnikprovozovatelvozidla.json vypisvozidel.json-140k vozidlavyrazenazprovozu.json zpravyvyrobcezastupce.json technickeprohlidky.json vozidladoplnkovevybaveni.json; 
do 
	#etl -config "$i" -metrics-backend datadog -v -validate; 
	fi=$(grep path $i | cut -f2 -d":" | sed 's/"//g' | sed 's/ //g'); 
	head -n 1000000 "$fi" > "${fi}-1000000"; 
#	rm "${i}-100000";
done

