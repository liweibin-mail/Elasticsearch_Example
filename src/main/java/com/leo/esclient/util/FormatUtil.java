package com.leo.esclient.util;

/**
 * Created by liweibin on 2017/1/24 0024.
 */
public class FormatUtil {

    /**
     * 打印输入到控制台
     *
     * @param jsonStr
     */
    public static void printJson(String jsonStr) {
        System.out.println(formatJson(jsonStr));
    }

    /**
     * 格式化
     *
     * @param jsonStr
     * @return
     */
    public static String formatJson(String jsonStr) {
        if (jsonStr == null || jsonStr.length() < 1) return "";
        jsonStr = jsonStr.replaceAll("\n", "");
        StringBuilder sb = new StringBuilder();
        char pre = '\0';
        int indent = 0;
        for (char c : jsonStr.toCharArray()) {
            if (pre == '\\') {
                sb.append(c);
                pre = c;
                continue;
            }
            switch (c) {
                case '{':
                case '[':
                    sb.append(c);
                    sb.append("\n");
                    addIndentBlank(sb, ++indent);
                    break;
                case ']':
                case '}':
                    sb.append("\n");
                    addIndentBlank(sb, --indent);
                    sb.append(c);
                    break;
                case ',':
                    sb.append(c);
                    sb.append("\n");
                    addIndentBlank(sb, indent);
                    break;
                default:
                    sb.append(c);
            }
            pre = c;
        }

        return sb.toString();
    }

    /**
     * 添加space
     *
     * @param sb
     * @param indent
     */
    private static void addIndentBlank(StringBuilder sb, int indent) {
        if (indent < 1) {
            return;
        }
        while(indent > 0) {
            sb.append("\t");
            indent--;
        }
    }
}
