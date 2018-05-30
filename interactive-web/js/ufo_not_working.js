$(document).ready(function() {
    console.log(ufo_data);
    
    $('#ufo-table').DataTable({
        ajax: ufo_data,
        columns: [ 
           { data: 'datetime' },
           { data: 'durationMinutes' } 
        ]
    });
});

function make_first_upper(str) {
		var words = str.split(" ");
		for (var i = 0; i < words.length; i++ ) {
        var j = words[i].charAt(0).toUpperCase();
        words[i] = j + words[i].substr(1);
    }
    console.log(words);
    return words.join(" ");
}