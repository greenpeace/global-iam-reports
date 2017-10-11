function multiParameterToXml(parameterName) {
	var values = reportContext.getParameterValue(parameterName);
	if (values && values.length > 0) {
		return '<value>'+values.join('</value><value>')+'</value>';
	}
	return '';
}