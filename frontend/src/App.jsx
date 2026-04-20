import { useEffect, useState } from 'react'
import { AlertCircle, FileSpreadsheet, RefreshCw } from 'lucide-react'
import FileUpload from './components/FileUpload'
import ProgressBar from './components/ProgressBar'
import Summary from './components/Summary'
import { MAX_FILE_SIZE_MB } from './config'
import { fileAPI } from './services/api'

function triggerDirectDownload(downloadUrl, filename) {
  const link = document.createElement('a')
  link.href = downloadUrl
  link.download = filename
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

const FileProcessingSection = ({
  title,
  description,
  downloadPrefix,
  processFile,
  onFileProcessed,
  isValidFile,
  invalidFileMessage,
}) => {
  const [file, setFile] = useState(null)
  const [processing, setProcessing] = useState(false)
  const [progress, setProgress] = useState(0)
  const [status, setStatus] = useState('')
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [downloading, setDownloading] = useState(false)

  const handleFileSelect = async (selectedFile) => {
    if (isValidFile && !isValidFile(selectedFile)) {
      setError(invalidFileMessage || 'File non valido per questa sezione.')
      setFile(null)
      setResult(null)
      setProcessing(false)
      setProgress(0)
      setStatus('')
      return
    }
    setFile(selectedFile)
    setError(null)
    setResult(null)
    setProcessing(true)
    setProgress(0)
    setStatus('Caricamento file...')

    try {
      const response = await processFile(selectedFile, (uploadProgress) => {
        setProgress(uploadProgress)
        if (uploadProgress === 100) {
          setStatus('Elaborazione in corso...')
        }
      })

      setStatus('Completato!')
      setResult(response)
      setProcessing(false)
      onFileProcessed?.(selectedFile)
    } catch (err) {
      setError(err.message)
      setProcessing(false)
      setFile(null)
    }
  }

  const handleDownload = async () => {
    if (!result?.file_id) return

    setDownloading(true)
    try {
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
      const downloadUrl = fileAPI.getDownloadUrl(result.file_id)
      triggerDirectDownload(downloadUrl, `File_Riepilogativo_${downloadPrefix}_${timestamp}.xlsx`)
    } catch {
      setError('Errore durante il download: file non valido o corrotto')
    } finally {
      setDownloading(false)
    }
  }

  const handleReset = () => {
    setFile(null)
    setResult(null)
    setError(null)
    setProcessing(false)
    setProgress(0)
    setStatus('')
  }

  return (
    <div className="bg-white rounded-2xl shadow-xl p-8 space-y-8">
      <div className="space-y-2">
        <div className="flex items-center gap-3">
          <FileSpreadsheet className="w-8 h-8 text-blue-600" />
          <h2 className="text-2xl font-bold text-gray-800">{title}</h2>
        </div>
        {description ? <p className="text-gray-600">{description}</p> : null}
      </div>

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-sm font-medium text-red-800">{error}</p>
          </div>
          <button onClick={() => setError(null)} className="text-red-600 hover:text-red-800">
            ✕
          </button>
        </div>
      )}

      {!processing && !result && (
        <FileUpload onFileSelect={handleFileSelect} disabled={processing} />
      )}

      {processing && (
        <div className="space-y-6">
          <div className="flex items-center gap-3">
            <RefreshCw className="w-6 h-6 text-blue-600 animate-spin" />
            <h3 className="text-lg font-semibold text-gray-700">Elaborazione in corso...</h3>
          </div>
          <ProgressBar progress={progress} status={status} />
          <p className="text-sm text-gray-600 text-center">
            File: <span className="font-medium">{file?.name}</span>
          </p>
        </div>
      )}

      {result && !processing && (
        <div className="space-y-6">
          <Summary summary={result.summary} onDownload={handleDownload} downloading={downloading} title={title} />
          <div className="pt-6 border-t border-gray-200">
            <button
              onClick={handleReset}
              className="w-full md:w-auto px-6 py-2 text-gray-700 hover:text-gray-900 font-medium transition-colors duration-200 mx-auto block"
            >
              Elabora nuovo file
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

const CompareProcessingSection = ({ lastNfsFile, lastPisaFile }) => {
  const [nfsFile, setNfsFile] = useState(null)
  const [pisaFile, setPisaFile] = useState(null)
  const [processing, setProcessing] = useState(false)
  const [progress, setProgress] = useState(0)
  const [status, setStatus] = useState('')
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [downloading, setDownloading] = useState(false)

  const extractQuarterKey = (file) => {
    if (!file?.name) return null
    const name = file.name.toUpperCase()
    const match = name.match(/(I{1,3}|IV|[1-4])\s*°?\s*TRIM(?:ESTRE)?\.?\s*[-_ ]*(20\d{2})/)
    if (!match) return null
    const quarterRaw = match[1]
    const quarterMap = { I: 1, II: 2, III: 3, IV: 4 }
    const quarter = quarterMap[quarterRaw] || Number(quarterRaw)
    const year = Number(match[2])
    return `${year}-Q${quarter}`
  }

  const hasQuarterReference = (file) => Boolean(extractQuarterKey(file))

  const isLikelyNfsCompareFile = (file) => {
    if (!file?.name) return false
    const name = file.name.toLowerCase()
    const hasValidExt = name.endsWith('.csv') || name.endsWith('.xlsx') || name.endsWith('.xls')
    return name.includes('nfs') && name.includes('pagato') && hasValidExt
  }

  const isLikelyPisaCompareFile = (file) => {
    if (!file?.name) return false
    const name = file.name.toLowerCase()
    return name.includes('pisa') && name.includes('pagato') &&
      (name.endsWith('.xlsx') || name.endsWith('.xls'))
  }

  const handleNfsCompareFileSelect = (file) => {
    setNfsFile(file)
    setError(null)
  }

  const handlePisaCompareFileSelect = (file) => {
    setPisaFile(file)
    setError(null)
  }

  const handleUseLastFiles = () => {
    if (!lastNfsFile || !lastPisaFile) return
    const validNfs = isLikelyNfsCompareFile(lastNfsFile)
    const validPisa = isLikelyPisaCompareFile(lastPisaFile)

    if (validNfs) setNfsFile(lastNfsFile)
    if (validPisa) setPisaFile(lastPisaFile)

    if (!validNfs || !validPisa) {
      if (!validNfs) setNfsFile(null)
      if (!validPisa) setPisaFile(null)
      const missing = [
        !validNfs ? 'NFS trimestrale (.csv/.xlsx/.xls)' : null,
        !validPisa ? 'Pisa trimestrale (.xlsx/.xls)' : null,
      ].filter(Boolean)
      setError(`Ultimi file parzialmente validi: manca ${missing.join(' e ')}. Seleziona manualmente il file mancante.`)
      return
    }
    setError(null)
  }

  const handleCompare = async () => {
    const effectiveNfsFile = nfsFile
    const effectivePisaFile = pisaFile

    if (!effectiveNfsFile && !effectivePisaFile) {
      setError('Seleziona entrambi i file trimestrali per il confronto: NFS (.csv/.xlsx/.xls) e Pisa (.xlsx/.xls).')
      return
    }
    if (!effectiveNfsFile) {
      setError('Manca il file NFS per il confronto. Seleziona il file NFS Pagato trimestrale (.csv/.xlsx/.xls).')
      return
    }
    if (!effectivePisaFile) {
      setError('Manca il file Pisa per il confronto. Seleziona il file Pisa Pagato trimestrale (.xlsx/.xls).')
      return
    }
    if (!isLikelyNfsCompareFile(effectiveNfsFile)) {
      setError("File NFS non valido: usa un file trimestrale con nome 'NFS Pagato ... Trim...' (.csv/.xlsx/.xls).")
      return
    }
    if (!isLikelyPisaCompareFile(effectivePisaFile)) {
      setError("File Pisa non valido: usa un file Excel trimestrale con nome 'Pisa Pagato ... Trim...'.")
      return
    }
    const nfsQuarterKey = extractQuarterKey(effectiveNfsFile)
    const pisaQuarterKey = extractQuarterKey(effectivePisaFile)
    if (nfsQuarterKey && pisaQuarterKey && nfsQuarterKey !== pisaQuarterKey) {
      setError('Attenzione: i due file sembrano di trimestri diversi dal nome. Procedo comunque, verifica il risultato finale.')
    }

    // Auto-popola la UI con i file effettivamente usati nel confronto.
    if (!nfsFile) setNfsFile(effectiveNfsFile)
    if (!pisaFile) setPisaFile(effectivePisaFile)

    setError(null)
    setResult(null)
    setProcessing(true)
    setProgress(0)
    setStatus('Caricamento file...')

    try {
      const response = await fileAPI.processCompare(effectiveNfsFile, effectivePisaFile, (uploadProgress) => {
        setProgress(uploadProgress)
        if (uploadProgress === 100) {
          setStatus('Elaborazione in corso...')
        }
      })
      setStatus('Completato!')
      setResult(response)
    } catch (err) {
      const message = err?.message || 'Errore durante il confronto dei file'
      if (message.includes('Valuta Importo Mandato')) {
        setNfsFile(null)
        setResult(null)
        setError(`${message} Seleziona un file NFS trimestrale CSV con colonna importo mandato numerica.`)
      } else {
        setError(message)
      }
    } finally {
      setProcessing(false)
    }
  }

  const handleDownload = async () => {
    if (!result?.file_id) return

    setDownloading(true)
    try {
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
      const downloadUrl = fileAPI.getDownloadUrl(result.file_id)
      triggerDirectDownload(downloadUrl, `Confronto_NFS_FT_${timestamp}.xlsx`)
    } catch {
      setError('Errore durante il download: file non valido o corrotto')
    } finally {
      setDownloading(false)
    }
  }

  const handleReset = () => {
    setNfsFile(null)
    setPisaFile(null)
    setResult(null)
    setError(null)
    setProcessing(false)
    setProgress(0)
    setStatus('')
  }

  return (
    <div className="bg-white rounded-2xl shadow-xl p-8 space-y-8">
      <div className="space-y-2">
        <div className="flex items-center gap-3">
          <FileSpreadsheet className="w-8 h-8 text-blue-600" />
          <h2 className="text-2xl font-bold text-gray-800">Confronto</h2>
        </div>
        <p className="text-gray-600">Carica due file e genera un file Excel di confronto.</p>
      </div>

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-sm font-medium text-red-800">{error}</p>
          </div>
          <button onClick={() => setError(null)} className="text-red-600 hover:text-red-800">
            ✕
          </button>
        </div>
      )}

      {!processing && !result && (
        <div className="grid grid-cols-1 gap-6">
          {lastNfsFile && lastPisaFile && (
            <div className="flex flex-col gap-3">
              <button
                onClick={handleUseLastFiles}
                className="w-full md:w-auto px-6 py-2 border border-blue-600 text-blue-600 hover:bg-blue-50 font-semibold rounded-lg transition-colors duration-200"
              >
                Usa ultimi file caricati
              </button>
              <p className="text-sm text-gray-600">
                Ultimi file: <span className="font-medium">{lastNfsFile.name}</span> ·{' '}
                <span className="font-medium">{lastPisaFile.name}</span>
              </p>
            </div>
          )}
          <div className="space-y-2">
            <p className="text-sm font-medium text-gray-700">FT NFS</p>
            <FileUpload onFileSelect={handleNfsCompareFileSelect} disabled={processing} />
            {nfsFile && (
              <p className="text-sm text-gray-600">
                File selezionato: <span className="font-medium">{nfsFile.name}</span>
              </p>
            )}
          </div>
          <div className="space-y-2">
            <p className="text-sm font-medium text-gray-700">FT Pisa</p>
            <FileUpload onFileSelect={handlePisaCompareFileSelect} disabled={processing} />
            {pisaFile && (
              <p className="text-sm text-gray-600">
                File selezionato: <span className="font-medium">{pisaFile.name}</span>
              </p>
            )}
          </div>
          <button
            onClick={handleCompare}
            disabled={processing}
            className="w-full md:w-auto px-8 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white font-semibold rounded-lg transition-colors duration-200 mx-auto"
          >
            Confronta e genera file
          </button>
        </div>
      )}

      {processing && (
        <div className="space-y-6">
          <div className="flex items-center gap-3">
            <RefreshCw className="w-6 h-6 text-blue-600 animate-spin" />
            <h3 className="text-lg font-semibold text-gray-700">Elaborazione in corso...</h3>
          </div>
          <ProgressBar progress={progress} status={status} />
        </div>
      )}

      {result && !processing && (
        <div className="space-y-6">
          {/* PROTEZIONE: Verifica che summary esista prima di renderizzare */}
          {!result.summary ? (
            <div className="p-6 bg-yellow-50 border border-yellow-200 rounded-lg">
              <p className="text-yellow-800">
                ⚠️ Errore: dati di confronto non disponibili. Riprova il caricamento.
              </p>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="bg-blue-50 p-6 rounded-lg border border-blue-200 space-y-4">
                  <h3 className="font-semibold text-gray-700">NFS</h3>
                  <div className="space-y-2 text-sm text-gray-700">
                    <div className="flex justify-between">
                      <span>Cartacee</span>
                      <span className="font-medium">
                        {(result.summary.nfs?.cartacee?.count || 0).toLocaleString('it-IT')} ·{' '}
                        {(result.summary.nfs?.cartacee?.amount || 0).toLocaleString('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>Elettroniche</span>
                      <span className="font-medium">
                        {(result.summary.nfs?.elettroniche?.count || 0).toLocaleString('it-IT')} ·{' '}
                        {(result.summary.nfs?.elettroniche?.amount || 0).toLocaleString('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                    </div>
                  </div>
                </div>
                <div className="bg-purple-50 p-6 rounded-lg border border-purple-200 space-y-4">
                  <h3 className="font-semibold text-gray-700">Pisa</h3>
                  <div className="space-y-2 text-sm text-gray-700">
                    <div className="flex justify-between">
                      <span>Cartacee</span>
                      <span className="font-medium">
                        {(result.summary.pisa?.cartacee?.count || 0).toLocaleString('it-IT')} ·{' '}
                        {(result.summary.pisa?.cartacee?.amount || 0).toLocaleString('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>Elettroniche</span>
                      <span className="font-medium">
                        {(result.summary.pisa?.elettroniche?.count || 0).toLocaleString('it-IT')} ·{' '}
                        {(result.summary.pisa?.elettroniche?.amount || 0).toLocaleString('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
              <button
                onClick={handleDownload}
                disabled={downloading}
                className="w-full md:w-auto px-8 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white font-semibold rounded-lg transition-colors duration-200 flex items-center justify-center gap-2 mx-auto"
              >
                {downloading ? 'Download in corso...' : 'Scarica file confronto'}
              </button>
            </>
          )}
          <div className="pt-6 border-t border-gray-200">
            <button
              onClick={handleReset}
              className="w-full md:w-auto px-6 py-2 text-gray-700 hover:text-gray-900 font-medium transition-colors duration-200 mx-auto block"
            >
              Nuovo confronto
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

function App() {
  const [lastNfsFile, setLastNfsFile] = useState(null)
  const [lastPisaFile, setLastPisaFile] = useState(null)

  const isValidNfsPagatoFile = (file) => {
    if (!file?.name) return false
    const name = file.name.toLowerCase()
    if (name.includes('ricevut')) return false
    return name.endsWith('.csv') || name.endsWith('.xlsx') || name.endsWith('.xls')
  }

  const isValidPisaPagatoFile = (file) => {
    if (!file?.name) return false
    const name = file.name.toLowerCase()
    if (name.includes('ricevut')) return false
    return name.endsWith('.xlsx') || name.endsWith('.xls')
  }

  useEffect(() => {
    fileAPI.healthCheck().catch(() => {})
  }, [])

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 py-12 px-4">
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <div className="flex items-center justify-center gap-3 mb-4">
            <FileSpreadsheet className="w-12 h-12 text-blue-600" />
            <h1 className="text-4xl font-bold text-gray-800">1. Query Fatture NFS Trimestrali</h1>
          </div>
          <p className="text-gray-600">
            Elaborazione automatica file Excel trimestrali con filtraggio protocolli e note riepilogative
          </p>
        </div>

        <div className="grid grid-cols-1 gap-8">
          <FileProcessingSection
            title="FT NFS Pagato (Trimestrale)"
            description="Analisi e riepilogo per il file trimestrale NFS Pagato."
            downloadPrefix="FT_NFS_Pagato"
            processFile={fileAPI.processFile}
            onFileProcessed={setLastNfsFile}
            isValidFile={isValidNfsPagatoFile}
            invalidFileMessage="File NFS non valido: carica un file trimestrale NFS Pagato (.csv/.xlsx) e non un file Ricevute."
          />
          <FileProcessingSection
            title="FT Pisa Pagato (Trimestrale)"
            description="Analisi e riepilogo per il file trimestrale Pisa Pagato."
            downloadPrefix="FT_Pisa_Pagato"
            processFile={fileAPI.processFilePisa}
            onFileProcessed={setLastPisaFile}
            isValidFile={isValidPisaPagatoFile}
            invalidFileMessage="File Pisa non valido: carica un file trimestrale Pisa Pagato (.xlsx/.xls) e non un file Ricevute."
          />
          <CompareProcessingSection lastNfsFile={lastNfsFile} lastPisaFile={lastPisaFile} />
        </div>

        <div className="mt-8 text-center text-sm text-gray-600">
          <p>Versione 1.0.0 | Supporto: .xlsx, .csv | Max {MAX_FILE_SIZE_MB}MB</p>
        </div>
      </div>
    </div>
  )
}

export default App
