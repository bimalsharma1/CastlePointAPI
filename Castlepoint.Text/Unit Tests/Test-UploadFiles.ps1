
function Extract-TextFromFile([string]$filePath) {

    $fileName = [System.IO.Path]::GetFileName($filePath)

$baseurl = "http://localhost:6002/text/extract"

$multipartContent = [System.Net.Http.MultipartFormDataContent]::new()
$multipartFile = $filePath
$FileStream = [System.IO.FileStream]::new($multipartFile, [System.IO.FileMode]::Open)
$fileHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
$fileHeader.Name = "file"
$fileHeader.FileName = $fileName
$fileContent = [System.Net.Http.StreamContent]::new($FileStream)
$fileContent.Headers.ContentDisposition = $fileHeader
$fileContent.Headers.ContentType = [System.Net.Http.Headers.MediaTypeHeaderValue]::Parse("text/plain")
$multipartContent.Add($fileContent)

$stringHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
$stringHeader.Name = "mimetype"
$StringContent = [System.Net.Http.StringContent]::new("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
$StringContent.Headers.ContentDisposition = $stringHeader
$multipartContent.Add($stringContent)

$stringHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
$stringHeader.Name = "filename"
$StringContent = [System.Net.Http.StringContent]::new("Worksheet 001.xlsx")
$StringContent.Headers.ContentDisposition = $stringHeader
$multipartContent.Add($stringContent)

#Set POST content
#$body = @{}
#$body.Add("file", [IO.File]::ReadAllBytes($SourcePath))
#$body.Add("filename", "Worksheet 001.xlsx")
#$body.Add("mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

#Upload the file
Invoke-RestMethod -Uri $baseUrl -Method Post -Body $multipartContent
}
