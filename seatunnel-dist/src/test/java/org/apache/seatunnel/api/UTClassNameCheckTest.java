package org.apache.seatunnel.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.NodeList;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.ImportShadeClassCheckTest.isWindows;

@Slf4j
public class UTClassNameCheckTest {

    private final JavaParser JAVA_PARSER = new JavaParser();

    @Test
    public void checkUTClassName() {
        Path directoryPath = Paths.get(System.getProperty("user.dir")).getParent();
        String testPathFragment = isWindows ? "src\\test\\java" : "src/test/java";

        try (Stream<Path> paths = Files.walk(directoryPath)) {
            List<String> collect =
                    paths.filter(
                                    path -> {
                                        String pathString = path.toString();
                                        return pathString.endsWith(".java")
                                                && !pathString.contains("e2e")
                                                && pathString.contains(testPathFragment);
                                    })
                            .map(
                                    path -> {
                                        try {
                                            ParseResult<CompilationUnit> parseResult =
                                                    JAVA_PARSER.parse(Files.newInputStream(path));
                                            return parseResult
                                                    .getResult()
                                                    .map(
                                                            compilationUnit -> {
                                                                NodeList<ImportDeclaration>
                                                                        imports =
                                                                                compilationUnit
                                                                                        .getImports();
                                                                return imports.stream()
                                                                                .anyMatch(
                                                                                        i ->
                                                                                                "org.junit.jupiter.api.Test"
                                                                                                        .equals(
                                                                                                                i.getName()
                                                                                                                        .asString()))
                                                                        ? path
                                                                        : null;
                                                            })
                                                    .orElse(null);
                                        } catch (Exception e) {
                                            log.error("Error parsing file: {}", path, e);
                                            return null;
                                        }
                                    })
                            .filter(Objects::nonNull)
                            .filter(
                                    path -> {
                                        String fileName = path.getFileName().toString();
                                        int dotIndex = fileName.lastIndexOf('.');
                                        String className =
                                                dotIndex == -1
                                                        ? fileName
                                                        : fileName.substring(0, dotIndex);
                                        return !(className.startsWith("Test")
                                                || className.endsWith("Test")
                                                || className.endsWith("Tests")
                                                || className.endsWith("TestCase"));
                                    })
                            .map(Path::toAbsolutePath)
                            .map(Path::toString)
                            .collect(Collectors.toList());
            Assertions.assertEquals(
                    0,
                    collect.size(),
                    () ->
                            "UT class does not conform to the naming convention, "
                                    + "must should be start with 'Test' or end with 'Test' "
                                    + "or end with 'Tests' or end with 'TestCase'.\n "
                                    + String.join("\n", collect));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
