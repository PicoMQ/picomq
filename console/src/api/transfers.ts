import { admin, request } from './client'
import { listing } from './query'

export type Log = { topic: string; partition: number } | { group: string } | null

export interface Transfer {
  log: Log
  fromNode: number
  toNode: number
}

export interface TransferCreate {
  topic: string
  partition?: number
  toNode: number
}

export const transfers = () => listing<Transfer>(['transfers'])

export const createTransfer = (body: TransferCreate) =>
  request<Transfer>('POST', admin('transfers'), { body })
