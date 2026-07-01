{{- define "claimpipe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "claimpipe.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "claimpipe.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "claimpipe.labels" -}}
app.kubernetes.io/name: {{ include "claimpipe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}
