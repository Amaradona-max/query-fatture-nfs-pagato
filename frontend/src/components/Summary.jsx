import { Download, FileCheck, FileText } from 'lucide-react'

const Summary = ({ summary, onDownload, downloading, title }) => {
  // PROTEZIONE: Se summary è undefined o null, mostra un messaggio di fallback
  if (!summary) {
    return (
      <div className="w-full p-6 bg-yellow-50 border border-yellow-200 rounded-lg">
        <p className="text-yellow-800">
          ⚠️ Errore: dati di riepilogo non disponibili. Riprova il caricamento.
        </p>
      </div>
    )
  }

  return (
    <div className="w-full space-y-6">
      <div className="flex items-center gap-3">
        <FileCheck className="w-8 h-8 text-green-600" />
        <h2 className="text-2xl font-bold text-gray-800">
          Elaborazione Completata{title ? ` - ${title}` : ''}
        </h2>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-blue-50 p-6 rounded-lg border border-blue-200">
          <div className="flex items-center gap-3 mb-2">
            <FileText className="w-5 h-5 text-blue-600" />
            <h3 className="font-semibold text-gray-700">Record Totali</h3>
          </div>
          <p className="text-3xl font-bold text-blue-600">
            {(summary.total_records || 0).toLocaleString('it-IT')}
          </p>
          <p className="text-sm text-gray-600 mt-2">
            Duplicati rimossi: {(summary.duplicates_removed || 0).toLocaleString('it-IT')}
          </p>
        </div>

        <div className="bg-green-50 p-6 rounded-lg border border-green-200">
          <h3 className="font-semibold text-gray-700 mb-2">Fase 2 - Cartacee</h3>
          <p className="text-3xl font-bold text-green-600">
            {(summary.fase2_records || 0).toLocaleString('it-IT')}
          </p>
          <div className="mt-3 space-y-1 text-xs text-gray-600">
            {summary.protocols_fase2 && Object.entries(summary.protocols_fase2).map(([prot, count]) =>
              count > 0 ? (
                <div key={prot} className="flex justify-between">
                  <span>{prot}:</span>
                  <span className="font-medium">{count}</span>
                </div>
              ) : null
            )}
          </div>
        </div>

        <div className="bg-purple-50 p-6 rounded-lg border border-purple-200 md:col-span-2">
          <h3 className="font-semibold text-gray-700 mb-2">Fase 3 - Elettroniche</h3>
          <p className="text-3xl font-bold text-purple-600 mb-3">
            {(summary.fase3_records || 0).toLocaleString('it-IT')}
          </p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-1 text-xs text-gray-600">
            {summary.protocols_fase3 && Object.entries(summary.protocols_fase3).map(([prot, count]) =>
              count > 0 ? (
                <div key={prot} className="flex justify-between">
                  <span>{prot}:</span>
                  <span className="font-medium">{count}</span>
                </div>
              ) : null
            )}
          </div>
        </div>
      </div>

      <button
        onClick={onDownload}
        disabled={downloading}
        className="w-full md:w-auto px-8 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white font-semibold rounded-lg transition-colors duration-200 flex items-center justify-center gap-2 mx-auto"
      >
        <Download className="w-5 h-5" />
        {downloading ? 'Download in corso...' : 'Scarica File Elaborato'}
      </button>
    </div>
  )
}

export default Summary
