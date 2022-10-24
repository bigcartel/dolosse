{{- define "dolosse.deployment-labels" -}}
{{- $top := first . -}}
{{- $suffix := index . 1 -}}
app.kubernetes.io/name: {{ $top.Chart.Name }}-{{ $suffix }}
app.kubernetes.io/instance: {{ $top.Release.Name }}-{{ $suffix }}
app.kubernetes.io/managed-by: {{ $top.Release.Service }}
helm.sh/chart: {{ $top.Chart.Name }}-{{ $top.Chart.Version | replace "+" "_" }}
{{- end -}}
