# Funcoes comuns aos scripts PowerShell: descoberta da raiz do projeto, do executavel da
# Databricks CLI e do usuario logado (para montar o caminho base dos notebooks no workspace).
# Carregue com:  . "$PSScriptRoot\_comum.ps1"

function Get-RaizProjeto {
    # Os scripts ficam em scripts\; a raiz do projeto e o diretorio pai.
    return (Resolve-Path "$PSScriptRoot\..").Path
}

function Get-DatabricksCli {
    # 1) databricks no PATH
    $cmd = Get-Command databricks -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    # 2) instalacao padrao do instalador da CLI no Windows
    $fallback = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
    if (Test-Path $fallback) { return $fallback }
    # 3) nao encontrada
    throw "Databricks CLI nao encontrada. Instale a CLI (https://docs.databricks.com/dev-tools/cli/install.html) e rode 'databricks auth login --profile <perfil>'. Esperava 'databricks' no PATH ou '$fallback'."
}

function Get-WsBase {
    # Descobre /Users/<userName>/antt-lakehouse. Se -PastaWs for informada, usa-a como esta.
    param(
        [Parameter(Mandatory = $true)][string]$Dbx,
        [Parameter(Mandatory = $true)][string]$Perfil,
        [string]$PastaWs
    )
    if ($PastaWs) { return $PastaWs.TrimEnd('/') }
    $j = & $Dbx current-user me -o json --profile $Perfil 2>&1 | Out-String
    $userName = $null
    try { $userName = ($j | ConvertFrom-Json).userName } catch { }
    if (-not $userName) {
        throw "Nao foi possivel descobrir o usuario logado via 'databricks current-user me --profile $Perfil'. Rode 'databricks auth login --profile $Perfil' e tente de novo. Saida: $j"
    }
    return "/Users/$userName/antt-lakehouse"
}
