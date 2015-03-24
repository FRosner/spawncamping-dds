function isNumericArray(arr) {
	return arr.reduce(function(allPreviousValuesAreNumeric, currentValue) {
		return !isNaN(currentValue) && allPreviousValuesAreNumeric
	}, true);
}
