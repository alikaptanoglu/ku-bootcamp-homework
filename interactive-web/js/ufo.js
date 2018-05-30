$(document).ready(function() {
    console.log(ufo_data);

    $('#search').on("keyup", function() {
        var value = $(this).val().toLowerCase();
        $(".table tbody tr").filter(function() {
            $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1);
        });
    });

    build_table(ufo_data);

});


function get_columns() {
    return {
      columns : [
        {
          title: 'Date',
          html: function (row) { return row.datetime; }
        },
        {
          title: 'Duration',
          html: function (row) { return row.durationMinutes; }
        },
        {
          title: 'City',
          html: function (row) { return make_first_upper(row.city); }
        },
        {
          title: 'State',
          html: function (row) { return row.state.toUpperCase(); }
        },
        {
          title: 'Country',
          html: function (row) { return row.country.toUpperCase(); }
        },
        {
          title: 'Shape',
          html: function (row) { return make_first_upper(row.shape); }
        },
        {
          title: 'Comment',
          html: function (row) { return row.comments; }
        }
      ],
      data: null
    }
}

function build_table(ufo_data) {
    let parameters = get_columns();
    let table = d3.select('#ufo-table').append('table').attr('class', 'table table-striped');

    table.append('thead').append('tr')
         .selectAll('th')
         .data(parameters['columns']) 
         .enter()
         .append('th')
         .text(function (data) { return data.title; });

    table.append('tbody')
         .selectAll('tr') 
         .data(ufo_data)
         .enter()
         .append('tr')
         .selectAll('td')
         .data(function (row, i) {
            // evaluate column objects against the current row
            return parameters.columns.map(function (column) {
                var cell = {};
                d3.keys(column).forEach(function (k) {
                if (typeof (column[k]) === 'function') {
                    cell[k] = column[k](row, i)
                }
            });
            return cell;
         });
    }).enter()
      .append('td')
      .text(function (data) { return data.html; });
}

function make_first_upper(str) {
		var words = str.split(" ");
		for (var i = 0; i < words.length; i++ ) {
        var j = words[i].charAt(0).toUpperCase();
        words[i] = j + words[i].substr(1);
    }
    console.log(words);
    return words.join(" ");
}