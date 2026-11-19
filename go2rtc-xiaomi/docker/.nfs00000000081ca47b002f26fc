#!/bin/sh
# 启动前清理：本插件内置整合界面后，miwifinet 集成里旧的「监控画面」
# 侧边栏面板（cams-panel）已多余，这里在其注册代码前加短路条件。
# 幂等：仅在目标行存在时替换一次，重复执行无副作用。
PANEL=/config/custom_components/miwifinet/panel.py
if [ -f "$PANEL" ] && grep -q 'if not hass.data.get(f"{DOMAIN}_cams_panel_registered"):' "$PANEL"; then
    sed -i 's|if not hass.data.get(f"{DOMAIN}_cams_panel_registered"):|if False and not hass.data.get(f"{DOMAIN}_cams_panel_registered"):|' "$PANEL" \
        && echo "run.sh: 已禁用 miwifinet 旧监控画面面板注册"
fi
exec go2rtc -config /config/go2rtc.yaml
