<template>
  <div class="app">
    <header class="header">
      <h1>🚀 QuantaDB Client</h1>
    </header>

    <div class="connection-panel">
      <div class="connection-form">
        <input
          v-model="serverAddress"
          type="text"
          placeholder="Server address (e.g., 127.0.0.1:5432)"
          :disabled="isConnected"
        />
        <button @click="connect" :disabled="isConnected || !serverAddress">
          Connect
        </button>
        <button @click="disconnect" :disabled="!isConnected">
          Disconnect
        </button>
        <div :class="['status', isConnected ? 'connected' : 'disconnected']">
          {{ isConnected ? 'Connected' : 'Disconnected' }}
        </div>
      </div>
    </div>

    <div class="main-content">
      <div class="query-panel">
        <div class="query-input">
          <label for="query">SQL Query:</label>
          <textarea
            id="query"
            v-model="query"
            placeholder="Enter your SQL query here..."
            :disabled="!isConnected"
          ></textarea>
        </div>
        <div class="query-buttons">
          <button @click="executeQuery" :disabled="!isConnected || !query.trim()">
            Execute Query
          </button>
          <button @click="clearQuery" :disabled="!query.trim()">
            Clear
          </button>
        </div>
      </div>

      <div class="results-panel">
        <div class="results-header">
          <h3>Results</h3>
          <div v-if="loading" class="loading">
            <div class="spinner"></div>
            Executing...
          </div>
        </div>
        <div class="results-content">
          <div v-if="error" class="error-message">
            {{ error }}
          </div>
          <div v-else-if="successMessage" class="success-message">
            {{ successMessage }}
          </div>
          <div v-else-if="results && results.length > 0">
            <table class="results-table">
              <thead>
                <tr>
                  <th v-for="(_, index) in results[0].values" :key="index">
                    Column {{ index + 1 }}
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, rowIndex) in results" :key="rowIndex">
                  <td v-for="(value, colIndex) in row.values" :key="colIndex">
                    {{ formatValue(value) }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else>
            <p>No results to display. Execute a query to see results here.</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { invoke } from '@tauri-apps/api/tauri'

const serverAddress = ref('127.0.0.1:5432')
const isConnected = ref(false)
const query = ref('')
const results = ref<any[]>([])
const error = ref('')
const successMessage = ref('')
const loading = ref(false)

const connect = async () => {
  try {
    loading.value = true
    error.value = ''
    const result = await invoke('connect_to_server', { address: serverAddress.value })
    successMessage.value = result as string
    isConnected.value = true
  } catch (err) {
    error.value = err as string
  } finally {
    loading.value = false
  }
}

const disconnect = async () => {
  try {
    loading.value = true
    error.value = ''
    const result = await invoke('disconnect_from_server')
    successMessage.value = result as string
    isConnected.value = false
    results.value = []
  } catch (err) {
    error.value = err as string
  } finally {
    loading.value = false
  }
}

const executeQuery = async () => {
  if (!query.value.trim()) return

  try {
    loading.value = true
    error.value = ''
    successMessage.value = ''
    
    const result = await invoke('execute_query', { query: query.value })
    const queryResult = result as any
    
    if (queryResult.success) {
      successMessage.value = queryResult.message
      results.value = queryResult.data || []
    } else {
      error.value = queryResult.error || 'Query execution failed'
    }
  } catch (err) {
    error.value = err as string
  } finally {
    loading.value = false
  }
}

const clearQuery = () => {
  query.value = ''
  results.value = []
  error.value = ''
  successMessage.value = ''
}

const formatValue = (value: any) => {
  if (value === null || value === undefined) {
    return 'NULL'
  }
  if (typeof value === 'string') {
    return `"${value}"`
  }
  return String(value)
}

const checkConnection = async () => {
  try {
    isConnected.value = await invoke('is_connected') as boolean
  } catch (err) {
    console.error('Failed to check connection status:', err)
  }
}

onMounted(() => {
  checkConnection()
})
</script>
