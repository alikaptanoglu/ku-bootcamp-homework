// arrays of color, class, radius by magnitude
mag = [4, 5, 6, 7, 8, 9];
mag_label = ['0.1-3.9', '4.0-4.9', '5.0-5.9', '6.0-6.9', '7.0-7.9', '8+'];
mag_color = ['#3046a0', '#f7f260', '#f4b15f', '#f4725f', '#f45f64', '#ce040e'];
mag_class = ['Minor', 'Light', 'Moderate', 'Strong', 'Major', 'Great'];
mag_radius = [5000, 10000, 20000, 30000, 40000, 50000];


// significant earthquakes within the last 30 days worldwide
var url_bubble = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson'

d3.json(url_bubble, function(data) {
    var features = data['features'];
    console.log(features)

    // accumulate timestamps
    var timelines = [];

    // loop through features
    for (var i = 0; i < features.length; i++) {

        console.log(features[i].geometry.coordinates)
        
        // get properties
        var properties = features[i].properties;
        // get the coordinates
        var location = features[i].geometry;
        // build the popup text
        var popup_text = '<b>Date:</b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + (new Date(properties.time)).toString() + '<br>' +
                            '<b>Location:</b>&nbsp;&nbsp;&nbsp;&nbsp;' + properties.place + '<br>' +
                            '<b>Latitude:</b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + location.coordinates[1] + '<br>' +
                            '<b>Longitude:</b>&nbsp;&nbsp;' + location.coordinates[0] + '<br>' +
                            '<b>Magnitude:</b>&nbsp;&nbsp;' + properties.mag + '&nbsp;&nbsp;' + get_class(properties.mag)
                            ;
        console.log(properties.time, ' date ', new Date(properties.time))   

        // add circle for each location to the map
        circle = L.circle([location.coordinates[1], location.coordinates[0]], {
                    color: get_color(properties.mag),
                    fillColor: get_color(properties.mag),
                    fillOpacity: 0.7,
                    radius: get_radius(properties.mag)
                }).addTo(mymap)
                  .bindPopup(popup_text);


        // add a new marker to the cluster group and bind a pop-up
        layer_cluster.addLayer(L.marker([location.coordinates[1], location.coordinates[0]])
                     .bindPopup(properties.place));

        // accumulate timestamps
        timelines.push(new Date(properties.time));

    }

    // sort timestamps in ascending order
    var sorted_timelines = timelines.sort(function(a, b) {
        return a - b;
    });
    
    // create test layer for time slider
    var layer_timeline = L.geoJson(data);
    // use range property
    var slider_control = L.control.sliderControl({
        position: "bottomleft",
        layer: layer_timeline, 
        timeAttribute: sorted_timelines,
        range: true
    });
    // add slider to the map
    mymap.addControl(slider_control);
    //start slider
    slider_control.startSlider();


    // add legend 
    var legend = L.control({ position: 'bottomright' });
    legend.onAdd = function (map) {
        var div = L.DomUtil.create('div', 'legend');
        var labels = [];
        labels.push('<span>MAGNITUDE</span><br>');
        mag.forEach(function (limit, index) {
            labels.push('<span style="background-color: ' + mag_color[index] + '">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>&nbsp;&nbsp;<span>' + mag_label[index] + '</span>&nbsp;&nbsp;<br>');
        })

        div.innerHTML = '<div class="labels"><ul>' + labels.join('') + '</ul></div>';
        return div;
    };

    // add legend to map
    legend.addTo(mymap)
});



// determine color for each level of the magnitude
function get_color(m) {
    for (var i = 0; i < 5; i++) {
        if (parseInt(m) < mag[i]) {
            return mag_color[i];
        }
    }
    return '#ce040e';
}

// determine the class of each earthquake
function get_class(m) {
    for (var i = 0; i < 5; i++) {
        if (parseInt(m) < mag[i]) {
            return mag_class[i];
        }
    }
    return "Great";
}

// determine the radius of each circle
function get_radius(m) {
    for (var i = 0; i < 5; i++) {
        if (parseInt(m) < mag[i]) {
            return mag_radius[i];
        }
    }
    return 50000;
}



