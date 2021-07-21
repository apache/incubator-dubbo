/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.mojo;

import org.apache.commons.io.FileUtils;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.AdaptiveClassCodeGenerator;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Generate prebuilt adaptive classes of dubbo for native image
 */
@Mojo(name = "generate", defaultPhase = LifecyclePhase.GENERATE_SOURCES, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class GenerateAdaptiveMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}")
    private MavenProject project;

    @Parameter(defaultValue = "${project.build.directory}/generated-sources/dubbo-adaptive")
    private File topOutputDirectory;

    @Parameter(property = "ignoreDeprecated", defaultValue = "true")
    private boolean ignoreDeprecatedSpi;

    private static final String DUBBO_PACKAGE_NAME = "org.apache.dubbo";

    /**
     * The main entry point for this Mojo, it is responsible for generating prebuilt adaptive
     * of dubbo into the `topOutputDirectory` folder.
     *
     * @exception MojoExecutionException If a class in dubbo decorated by @SPI cannot be handle nicely
     * @exception MojoFailureException If an unexpected runtime error is thrown when executing this mojo
     */
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            List<Class<?>> spiDecoratedClasses = this.getSpiDecoratedClasses();
            this.generateAdaptiveCodes(spiDecoratedClasses);
        } catch (RuntimeException e) {
            throw new MojoFailureException("An unexpected runtime error was thrown", e);
        }
    }

    private List<Class<?>> getSpiDecoratedClasses() throws MojoExecutionException {
        List<Artifact> modules = this.getDubboModules();
        Set<String> classNames = this.getDubboClasses(modules);

        ClassLoader classLoader = this.getDubboClassLoader(modules);
        List<Class<?>> spiDecoratedClasses = new ArrayList<>();
        for (String name : classNames) {
            try {
                Class<?> clz = Class.forName(name, true, classLoader);

                boolean isNotSpi = clz.getAnnotation(SPI.class) == null;
                if (isNotSpi) {
                    continue;
                }

                boolean isDeprecatedSpi = clz.getAnnotation(Deprecated.class) != null;
                if (isDeprecatedSpi && this.ignoreDeprecatedSpi) {
                    getLog().debug("Ignoring deprecated " + clz.getName());
                    continue;
                }

                for (Method method : clz.getMethods()) {
                    boolean isAdaptiveMethod = method.getAnnotation(Adaptive.class) != null;
                    if (isAdaptiveMethod) {
                        spiDecoratedClasses.add(clz);
                        getLog().debug("Found adaptive method " + clz.getName() + "::" + method.getName() + "()");
                        break;
                    }
                }
            } catch (Throwable e) {
                // Ignore errors on loading classes
            }
        }

        return spiDecoratedClasses;
    }

    private void generateAdaptiveCodes(List<Class<?>> spiDecoratedClasses) throws MojoExecutionException {
        project.addCompileSourceRoot(topOutputDirectory.getAbsolutePath());
        int successCount = 0;

        for (Class<?> clz : spiDecoratedClasses) {
            String value = getSpiValue(clz);
            String code = new AdaptiveClassCodeGenerator(clz, value).generate();
            File adaptiveFile = getAdaptiveFile(clz);
            try {
                FileUtils.writeStringToFile(adaptiveFile, code, StandardCharsets.UTF_8.name());
                getLog().debug("Generated adaptive " + clz.getName() + " to " + adaptiveFile.getPath());
                successCount += 1;
            } catch (IOException e) {
                String msg = String.format("Failed to write adaptive %s to %s", clz.getName(), adaptiveFile.getPath());
                throw new MojoExecutionException(msg);
            }
        }
        getLog().info("Generated " + successCount + " adaptive to " + topOutputDirectory.getPath());
    }

    private List<Artifact> getDubboModules() {
        List<Artifact> result = new ArrayList<>();
        for (Object obj : project.getRuntimeArtifacts()) {
            Artifact module = (Artifact) obj;
            if (module.getGroupId().startsWith(DUBBO_PACKAGE_NAME)) {
                result.add(module);
            }
        }
        return result;
    }

    private Set<String> getDubboClasses(List<Artifact> modules) throws MojoExecutionException {
        Set<String> result = new HashSet<>();

        for (Artifact mod : modules) {
            try {
                JarFile jar = new JarFile(mod.getFile());
                result.addAll(findClassesInJar(jar));
            } catch (IOException e) {
                String msg = String.format("Cannot handle JAR of %s at %s", mod.getArtifactId(), mod.getFile().getPath());
                throw new MojoExecutionException(msg);
            }
        }

        return result;
    }

    private ClassLoader getDubboClassLoader(List<Artifact> modules) throws MojoExecutionException {
        List<URL> urls = new ArrayList<>();
        for (Artifact module : modules) {
            try {
                urls.add(module.getFile().toURI().toURL());
            } catch (MalformedURLException e) {
                String msg = String.format("Cannot convert path of %s to URL (%s)", module.getArtifactId(), module.getFile().getPath());
                throw new MojoExecutionException(msg);
            }
        }
        return new URLClassLoader(urls.toArray(new URL[0]), this.getClass().getClassLoader());
    }

    private Set<String> findClassesInJar(JarFile jar) {
        Set<String> result = new HashSet<>();
        Enumeration<JarEntry> entry = jar.entries();
        final String dubboPath = DUBBO_PACKAGE_NAME.replace(".", "/");

        while (entry.hasMoreElements()) {
            JarEntry jarEntry = entry.nextElement();
            String path = jarEntry.getName();
            if (path.charAt(0) == '/') {
                path = path.substring(1);
            }

            if (jarEntry.isDirectory() || !path.startsWith(dubboPath) || !path.endsWith(".class")) {
                continue;
            }

            String className = path.substring(0, path.length() - 6).replace("/", ".");
            result.add(className);
        }
        return result;
    }

    private String getSpiValue(Class<?> clz) {
        SPI spi = clz.getAnnotation(SPI.class);
        String value = spi.value();
        if (StringUtils.isEmpty(value)) {
            value = "adaptive";
        }
        return value;
    }

    private File getAdaptiveFile(Class<?> clz) {
        String pkgPath = topOutputDirectory + File.separator + clz.getPackage().getName().replace(".", "/");
        File adaptiveFile = new File(pkgPath + File.separator + clz.getSimpleName() + "$Adaptive.java");
        adaptiveFile.getParentFile().mkdirs();
        return adaptiveFile;
    }

}
