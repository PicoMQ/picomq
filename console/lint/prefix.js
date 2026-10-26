import { basename, dirname } from 'node:path'

import stylelint from 'stylelint'

const ruleName = 'picomq/class-prefix'

const messages = stylelint.utils.ruleMessages(ruleName, {
  prefixed: (selector, prefix) => `"${selector}" must use classes named "${prefix}" or "${prefix}-*"`,
  shared: (selector) => `"${selector}" is in a shared style file, which styles elements only`,
})

const rule = (enabled) => (root, result) => {
  const file = root.source?.input.file
  if (!enabled || !file) {
    return
  }

  const shared = basename(dirname(file)) === 'styles'
  const prefix = basename(file, '.css')

  root.walkRules((node) => {
    for (const [, name] of node.selector.matchAll(/\.([\w-]+)/g)) {
      const owned = name === prefix || name.startsWith(`${prefix}-`)
      if (shared || !owned) {
        stylelint.utils.report({
          ruleName,
          result,
          node,
          message: shared ? messages.shared(node.selector) : messages.prefixed(node.selector, prefix),
        })
      }
    }
  })
}

rule.ruleName = ruleName
rule.messages = messages

export default stylelint.createPlugin(ruleName, rule)
