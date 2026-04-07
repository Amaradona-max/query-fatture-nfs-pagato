import axios from 'axios'

const DEV_HOSTNAME =
  typeof window !== 'undefined' && window.location?.hostname
    ? window.location.hostname
    : 'localhost'

const API_BASE_URL = import.meta.env.PROD
  ? (import.meta.env.VITE_API_URL || 'https://nfs-ft-backend.onrender.com')
  : `http://${DEV_HOSTNAME}:8000`

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'multipart/form-data',
  },
})

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function getErrorMessage(error, fallbackMessage) {
  const detail = error?.response?.data?.detail
  if (!detail) return fallbackMessage
  if (typeof detail === 'string') return detail
  if (Array.isArray(detail)) {
    const msg = detail
      .map((d) => (typeof d?.msg === 'string' ? d.msg : typeof d === 'string' ? d : null))
      .filter(Boolean)
      .join(' | ')
    return msg || fallbackMessage
  }
  try {
    return JSON.stringify(detail)
  } catch {
    return fallbackMessage
  }
}

async function pollTask(taskId, onProgress) {
  let p = 0
  while (true) {
    await sleep(1500)
    const res = await api.get(`/api/task/${taskId}`)
    const { status, summary, error, file_id, download_url } = res.data
    if (status === 'done') {
      onProgress?.(100)
      return { success: true, file_id: file_id || taskId, summary, download_url }
    }
    if (status === 'error') {
      throw new Error(error || 'Errore durante l’elaborazione del file')
    }
    p = Math.min(90, p + 5)
    onProgress?.(p)
  }
}

export const fileAPI = {
  processFile: async (file, onProgress) => {
    const formData = new FormData()
    formData.append('file', file)

    try {
      const response = await api.post('/api/process-file', formData, {
        onUploadProgress: (progressEvent) => {
          if (!progressEvent.total) return
          const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total)
          onProgress?.(Math.round(percentCompleted * 0.4))
        },
      })
      const data = response.data
      if (data?.task_id) {
        return await pollTask(data.task_id, (p) => onProgress?.(40 + p * 0.6))
      }
      return data
    } catch (error) {
      throw new Error(getErrorMessage(error, 'Errore durante il caricamento del file'))
    }
  },
  processFilePisa: async (file, onProgress) => {
    const formData = new FormData()
    formData.append('file', file)

    try {
      const response = await api.post('/api/process-file-pisa', formData, {
        onUploadProgress: (progressEvent) => {
          if (!progressEvent.total) return
          const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total)
          onProgress?.(Math.round(percentCompleted * 0.4))
        },
      })
      const data = response.data
      if (data?.task_id) {
        return await pollTask(data.task_id, (p) => onProgress?.(40 + p * 0.6))
      }
      return data
    } catch (error) {
      throw new Error(getErrorMessage(error, 'Errore durante il caricamento del file'))
    }
  },
  processCompare: async (fileNfs, filePisa, onProgress) => {
    const formData = new FormData()
    formData.append('file_nfs', fileNfs)
    formData.append('file_pisa', filePisa)
    formData.append('nfs_file', fileNfs)
    formData.append('pisa_file', filePisa)

    try {
      const response = await api.post('/api/process-compare', formData, {
        onUploadProgress: (progressEvent) => {
          if (!progressEvent.total) return
          const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total)
          onProgress?.(Math.round(percentCompleted * 0.4))
        },
      })
      const data = response.data
      if (data?.task_id) {
        return await pollTask(data.task_id, (p) => onProgress?.(40 + p * 0.6))
      }
      return data
    } catch (error) {
      throw new Error(getErrorMessage(error, 'Errore durante il confronto dei file'))
    }
  },

  downloadFile: async (fileId) => {
    try {
      const response = await api.get(`/api/download/${fileId}`, {
        responseType: 'blob',
      })
      return response.data
    } catch {
      throw new Error('Errore durante il download del file')
    }
  },

  healthCheck: async () => {
    const response = await api.get('/api/health')
    return response.data
  },
  closeDay: async (message) => {
    try {
      const response = await api.post(
        '/api/close-day',
        { message },
        {
          headers: {
            'Content-Type': 'application/json',
          },
        }
      )
      return response.data
    } catch (error) {
      throw new Error(getErrorMessage(error, 'Errore durante la chiusura della giornata'))
    }
  },
}

export default api
