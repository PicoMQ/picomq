import { Input, Select } from './control'

const CHOICES = 100

interface Props {
  partitions: number
  value: number
  prefix?: string
  onChange: (index: number) => void
}

export function PartitionPicker({ partitions, value, prefix, onChange }: Props) {
  if (partitions > CHOICES) {
    return (
      <Input
        prefix={prefix}
        type="number"
        min={0}
        max={partitions - 1}
        size={6}
        value={String(value)}
        onChange={(event) => onChange(Number(event.currentTarget.value))}
      />
    )
  }
  return (
    <Select prefix={prefix} value={String(value)} onChange={(event) => onChange(Number(event.currentTarget.value))}>
      {Array.from({ length: partitions }, (_, each) => (
        <option key={each} value={String(each)}>
          {each}
        </option>
      ))}
    </Select>
  )
}
