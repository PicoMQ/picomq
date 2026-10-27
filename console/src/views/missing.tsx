import { Notice } from '../components/notice'
import { View } from '../components/view'

export default function Missing() {
  return (
    <View trail={[{ label: 'No such page' }]}>
      <Notice>
        Nothing lives at this address. Pick a section on the left, or go to the <a href="/">overview</a>.
      </Notice>
    </View>
  )
}
