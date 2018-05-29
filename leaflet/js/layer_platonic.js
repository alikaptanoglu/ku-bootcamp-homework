// load geojson files for the platonic layers
var json_boundaries = '../homework-leaflet/tectonicplates/GeoJSON/PB2002_orogens.json';

// var json_data = JSON.parse(json_boundaries);
// console.log(json_data);

$.getJSON(json_boundaries, function(data) {
    console.log(JSON.stringify(data));
});


