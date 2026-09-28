import { afterEach } from 'vitest'
import { clearSession } from '../src/api/tokenStorage'

afterEach(() => {
  clearSession()
})
