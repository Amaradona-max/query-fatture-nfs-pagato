import { useCallback, useState } from 'react'
import { useDropzone } from 'react-dropzone'
import { AlertCircle, FileSpreadsheet, Upload } from 'lucide-react'

const FileUpload = ({ onFileSelect, disabled }) => {
  const [error, setError] = useState(null)

  const onDrop = useCallback(
    (acceptedFiles, rejectedFiles) => {
      setError(null)

      if (rejectedFiles.length > 0) {
        const rejection = rejectedFiles[0]
        if (rejection.errors[0]?.code === 'file-too-large') {
          setError('File troppo grande. Dimensione massima: 60MB')
        } else if (rejection.errors[0]?.code === 'file-invalid-type') {
          setError('Formato file non valido. Carica un file .xlsx')
        } else {
          setError('File non valido')
        }
        return
      }

      if (acceptedFiles.length > 0) {
        onFileSelect(acceptedFiles[0])
      }
    },
    [onFileSelect]
  )

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
    },
    maxSize: 62914560,
    maxFiles: 1,
    disabled,
  })

  return (
    <div className="w-full">
      <div
        {...getRootProps()}
        className={`border-2 border-dashed rounded-lg p-12 text-center cursor-pointer transition-colors duration-200 ${
          isDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
        } ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
      >
        <input {...getInputProps()} />
        <div className="flex flex-col items-center gap-4">
          {isDragActive ? (
            <FileSpreadsheet className="w-16 h-16 text-blue-500" />
          ) : (
            <Upload className="w-16 h-16 text-gray-400" />
          )}
          <div className="space-y-2">
            <p className="text-lg font-medium text-gray-700">
              {isDragActive
                ? 'Rilascia il file qui'
                : 'Trascina qui il file Excel oppure clicca per selezionarlo'}
            </p>
            <p className="text-sm text-gray-500">
              ✓ Formati supportati: .xlsx
              <br />
              ✓ Dimensione massima: 60MB
            </p>
          </div>
        </div>
      </div>

      {error && (
        <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
          <p className="text-sm text-red-800">{error}</p>
        </div>
      )}
    </div>
  )
}

export default FileUpload
