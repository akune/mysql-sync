package de.kune.mysqlsync.anonymizer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenericAnonymizer implements FieldAnonymizer {

    public static final Pattern EXPRESSION_PATTERN = Pattern.compile("(?<expr>\\$\\{(?<key>.*?)})");
    private final String expression;

    public GenericAnonymizer(String expression) {
        this.expression = expression;
    }

    @Override
    public String anonymize(String key, Object value, Map<String, Object> context) {
        String result = eval(expression, key, value, context);
        return result;
    }

    private String eval(String expression, String key, Object value, Map<String, Object> context) {
        String x = expression;
        Matcher matcher = EXPRESSION_PATTERN.matcher(x);
        while (matcher.find()) {
            x = x.replace(matcher.group("expr"), Optional.ofNullable(context.get(matcher.group("key"))).map(Object::toString).orElse("NULL"));
            matcher = EXPRESSION_PATTERN.matcher(x);
        }
        return x;
    }

}
